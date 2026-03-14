import express from "express";
import fetch from "node-fetch";
import dotenv from "dotenv";
import { promises as fs } from "fs";
import path from "path";

dotenv.config();

const app = express();
app.use(express.static("public"));
app.use(express.json({ limit: "1mb" }));

const dataDir = path.join(process.cwd(), "data");
const respondioStatsFile = path.join(dataDir, "respondio-stats.json");

async function getJSON(url) {
  const r = await fetch(url);
  const text = await r.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!r.ok) {
    const err = new Error(`HTTP ${r.status}: ${JSON.stringify(data)}`);
    err.httpStatus = r.status;
    err.responseData = data;
    throw err;
  }
  return data;
}

async function postJSON(url, body, headers = {}) {
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...headers,
    },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!r.ok) {
    const err = new Error(`HTTP ${r.status}: ${JSON.stringify(data)}`);
    err.httpStatus = r.status;
    err.responseData = data;
    throw err;
  }
  return data;
}

function mapMetaError(e) {
  const metaErr = e?.responseData?.error;
  if (!metaErr) return null;

  if (metaErr.code === 190) {
    const isExpired = metaErr.error_subcode === 463;

    return {
      httpStatus: 401,
      payload: {
        error: metaErr.message || "Meta access token is invalid or expired.",
        token_expired: isExpired,
        token_invalid: !isExpired,
        meta_error_code: metaErr.code,
        meta_error_subcode: metaErr.error_subcode ?? null,
        action: isExpired
          ? "Generate a new long-lived user token and replace USER_ACCESS_TOKEN in .env, then restart the server."
          : "Replace USER_ACCESS_TOKEN with a valid token that has access to the configured IG account, then restart the server.",
      },
    };
  }

  return {
    httpStatus: e.httpStatus || 502,
    payload: {
      error: metaErr.message || "Meta Graph API request failed.",
      meta_error_code: metaErr.code ?? null,
      meta_error_subcode: metaErr.error_subcode ?? null,
    },
  };
}

function isPermissionError(e) {
  return e?.responseData?.error?.code === 10;
}

function isPageTokenRequiredError(e) {
  const err = e?.responseData?.error;
  return (
    err?.code === 190 &&
    typeof err?.message === "string" &&
    err.message.includes("Page Access Token")
  );
}

async function fetchInsightsBestEffort(baseUrl, metrics) {
  const results = await Promise.all(
    metrics.map(async (metric) => {
      const url = `${baseUrl}&metric=${encodeURIComponent(metric)}`;
      try {
        const data = await getJSON(url);
        const item = Array.isArray(data?.data) ? data.data[0] : null;
        return item || null;
      } catch (e) {
        const metaErr = e?.responseData?.error;
        // Ignore unsupported metric errors so one bad metric does not fail all KPIs.
        if (metaErr?.code === 100) return null;
        throw e;
      }
    })
  );

  return { data: results.filter(Boolean) };
}

function firstAvailableMetric(insightsData, metricNames) {
  for (const name of metricNames) {
    const v = sumMetricValues(insightsData, name);
    if (Number.isFinite(v)) return v;
  }
  return null;
}

async function fetchInsightsTotalValueBestEffort(baseUrl, metrics) {
  const results = await Promise.all(
    metrics.map(async (metric) => {
      const url = `${baseUrl}&metric=${encodeURIComponent(metric)}`;
      try {
        const data = await getJSON(url);
        const item = Array.isArray(data?.data) ? data.data[0] : null;
        const value = item?.total_value?.value;
        if (typeof value === "number") return { name: metric, value };
        return null;
      } catch (e) {
        const metaErr = e?.responseData?.error;
        // Ignore unsupported metric errors so one bad metric does not fail all KPIs.
        if (metaErr?.code === 100) return null;
        throw e;
      }
    })
  );

  const out = {};
  for (const r of results) {
    if (r) out[r.name] = r.value;
  }
  return out;
}

function extractLinkClicksFromPostClicksByType(value) {
  if (!value || typeof value !== "object") return null;
  let total = 0;
  let found = false;
  for (const [k, v] of Object.entries(value)) {
    if (!Number.isFinite(v)) continue;
    // Graph can return keys like "link clicks" (or similar variants).
    if (k.toLowerCase().includes("link")) {
      total += v;
      found = true;
    }
  }
  return found ? total : null;
}

async function fetchFacebookLinkClicksFromPosts(pageId, token, since, until) {
  let nextUrl =
    `https://graph.facebook.com/v21.0/${pageId}/posts` +
    `?since=${since}&until=${until}&limit=100&fields=id` +
    `&access_token=${encodeURIComponent(token)}`;
  let totalLinkClicks = 0;
  let processedPosts = 0;
  let pages = 0;

  while (nextUrl && pages < 10) {
    const postsData = await getJSON(nextUrl);
    const posts = Array.isArray(postsData?.data) ? postsData.data : [];
    pages += 1;

    for (const post of posts) {
      const postId = post?.id;
      if (!postId) continue;
      processedPosts += 1;
      try {
        const insightsUrl =
          `https://graph.facebook.com/v21.0/${postId}/insights` +
          `?metric=post_clicks_by_type` +
          `&access_token=${encodeURIComponent(token)}`;
        const insights = await getJSON(insightsUrl);
        const item = Array.isArray(insights?.data) ? insights.data[0] : null;
        const values = Array.isArray(item?.values) ? item.values : [];
        for (const entry of values) {
          const clicks = extractLinkClicksFromPostClicksByType(entry?.value);
          if (Number.isFinite(clicks)) totalLinkClicks += clicks;
        }
      } catch (e) {
        // Skip individual post insight failures to keep aggregation resilient.
      }
    }

    nextUrl = postsData?.paging?.next || null;
  }

  if (processedPosts === 0) return 0;
  return totalLinkClicks;
}

async function getPageAccessTokenFromUserToken(userToken, pageId) {
  if (!userToken || !pageId) return null;
  const url =
    `https://graph.facebook.com/v21.0/me/accounts` +
    `?fields=id,access_token&limit=200` +
    `&access_token=${encodeURIComponent(userToken)}`;
  const data = await getJSON(url);
  const page = Array.isArray(data?.data) ? data.data.find((p) => String(p?.id) === String(pageId)) : null;
  return page?.access_token || null;
}

function unixSeconds(d) {
  return Math.floor(d.getTime() / 1000);
}

function weekRangeUTC(weekOffset = 0) {
  // Returns [since, untilExclusive] for a Sunday..Saturday week in UTC.
  // weekOffset=0 -> current week, 1 -> previous week, ...
  const now = new Date();
  const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const dayOfWeek = todayUtc.getUTCDay(); // 0=Sun ... 6=Sat
  const sunday = new Date(todayUtc);
  sunday.setUTCDate(todayUtc.getUTCDate() - dayOfWeek - weekOffset * 7);
  const nextSunday = new Date(sunday);
  nextSunday.setUTCDate(sunday.getUTCDate() + 7);
  return [unixSeconds(sunday), unixSeconds(nextSunday), sunday, nextSunday];
}

function metricValues(insightsData, metricName) {
  const item = (insightsData || []).find((m) => m.name === metricName);
  if (!item || !Array.isArray(item.values) || item.values.length === 0) return [];
  return item.values
    .map((v) => (typeof v?.value === "number" ? v.value : null))
    .filter((v) => v != null);
}

function sumMetricValues(insightsData, metricName) {
  const values = metricValues(insightsData, metricName);
  if (!values.length) return null;
  return values.reduce((acc, n) => acc + n, 0);
}

function followerDeltaForPeriod(insightsData) {
  const item = (insightsData || []).find((m) => m.name === "follower_count");
  if (!item || !Array.isArray(item.values) || item.values.length < 2) return null;
  const first = item.values[0]?.value;
  const last = item.values[item.values.length - 1]?.value;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
  return last - first;
}

function metricDeltaForPeriod(insightsData, metricName) {
  const item = (insightsData || []).find((m) => m.name === metricName);
  if (!item || !Array.isArray(item.values) || item.values.length < 2) return null;
  const first = item.values[0]?.value;
  const last = item.values[item.values.length - 1]?.value;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
  return last - first;
}

function pct(n) {
  return Number.isFinite(n) ? Math.round(n * 10000) / 100 : null; // 2 decimals
}

function safeDiv(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  return a / b;
}

function parseOptionalInt(value) {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function safeNumber(value) {
  return Number.isFinite(value) ? value : null;
}

function delta(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous)) return null;
  return current - previous;
}

function compactMetrics(source, keys) {
  const out = {};
  for (const key of keys) out[key] = safeNumber(source?.[key]);
  return out;
}

function formatRespondDateUTC(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const mm = String(date.getUTCMinutes()).padStart(2, "0");
  const ss = String(date.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${d} ${hh}:${mm}:${ss}`;
}

async function ensureDataDir() {
  await fs.mkdir(dataDir, { recursive: true });
}

async function readRespondioStats() {
  try {
    const raw = await fs.readFile(respondioStatsFile, "utf8");
    const parsed = JSON.parse(raw);
    return {
      messages_received: Number.isFinite(parsed?.messages_received) ? parsed.messages_received : 0,
      webhook_events: Number.isFinite(parsed?.webhook_events) ? parsed.webhook_events : 0,
      last_message_at: parsed?.last_message_at ?? null,
      last_event_at: parsed?.last_event_at ?? null,
    };
  } catch {
    return {
      messages_received: 0,
      webhook_events: 0,
      last_message_at: null,
      last_event_at: null,
    };
  }
}

async function writeRespondioStats(stats) {
  await ensureDataDir();
  await fs.writeFile(respondioStatsFile, JSON.stringify(stats, null, 2));
}

function isRespondioMessageEvent(payload) {
  if (!payload || typeof payload !== "object") return false;
  const type = String(payload.type || payload.eventType || payload.event || "").toLowerCase();
  if (type.includes("message")) return true;
  if (payload.message) return true;
  if (payload.messages && Array.isArray(payload.messages) && payload.messages.length > 0) return true;
  return false;
}

// ---- Simple server-side cache to avoid rate limiting
const CACHE_TTL_MS = 15000; // Meta call at most every 15s
let cache = { key: "", ts: 0, data: null };
const RESPOND_CACHE_TTL_MS = 60000;
let respondCache = { key: "", ts: 0, data: null };
const AI_SUMMARY_CACHE_TTL_MS = 5 * 60 * 1000;
let aiSummaryCache = { key: "", ts: 0, data: null };
const RESPOND_CONTACTS_PAGE_LIMIT = 50;

async function getRespondioWeeklyContacts(weekOffset = 0) {
  const token = process.env.RESPOND_ACCESS_TOKEN;
  if (!token) {
    const err = new Error("Missing RESPOND_ACCESS_TOKEN in .env");
    err.httpStatus = 400;
    throw err;
  }

  const safeWeekOffset = Number.isFinite(weekOffset) && weekOffset >= 0 ? weekOffset : 0;
  const cacheKey = `respond:weekly-contacts:${safeWeekOffset}`;
  const now = Date.now();
  if (respondCache.data && respondCache.key === cacheKey && now - respondCache.ts < RESPOND_CACHE_TTL_MS) {
    return { ...respondCache.data, cached: true };
  }

  const [since, until, weekStart, weekEndExclusive] = weekRangeUTC(safeWeekOffset);
  const weekEndInclusive = new Date(weekEndExclusive);
  weekEndInclusive.setUTCDate(weekEndInclusive.getUTCDate() - 1);

  let nextUrl = `https://api.respond.io/v2/contact/list?limit=${RESPOND_CONTACTS_PAGE_LIMIT}`;
  let pages = 0;
  const maxPages = 200;
  const uniqueContactIds = new Set();

  const requestBody = {
    search: "",
    timezone: "UTC",
    filter: { $and: [] },
  };

  while (nextUrl && pages < maxPages) {
    const data = await postJSON(nextUrl, requestBody, {
      Authorization: `Bearer ${token}`,
    });
    pages += 1;

    const items = Array.isArray(data?.items) ? data.items : [];
    let pageHasPotentialNewerItems = false;
    for (const item of items) {
      const id = item?.id;
      const createdAt = item?.created_at;
      if (!Number.isFinite(createdAt) || id == null) continue;
      if (createdAt >= since) pageHasPotentialNewerItems = true;
      if (createdAt >= since && createdAt < until) uniqueContactIds.add(String(id));
    }

    if (!pageHasPotentialNewerItems) break;
    nextUrl = data?.pagination?.next || null;
  }

  const payload = {
    cached: false,
    period: {
      type: "weekly",
      week_offset: safeWeekOffset,
      start_utc: weekStart.toISOString(),
      end_utc_inclusive: weekEndInclusive.toISOString(),
      label: `${weekStart.toISOString().slice(0, 10)} to ${weekEndInclusive.toISOString().slice(0, 10)} (Sun-Sat, UTC)`,
    },
    people_contacted: uniqueContactIds.size,
    source: "respond.io contacts created_at",
    pages_scanned: pages,
  };

  respondCache = { key: cacheKey, ts: now, data: payload };
  return payload;
}

async function getMetaSummary(weekOffset = 0) {
  const safeWeekOffset = Number.isFinite(weekOffset) && weekOffset >= 0 ? weekOffset : 0;
  const cacheKey = `summary:week_offset=${safeWeekOffset}`;
  const now = Date.now();
  if (cache.data && cache.key === cacheKey && now - cache.ts < CACHE_TTL_MS) {
    return { ...cache.data, cached: true };
  }

  const igToken = process.env.USER_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN;
  const fbToken = process.env.FB_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN || process.env.USER_ACCESS_TOKEN;
  const userTokenForPageLookup = process.env.USER_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN;
  const igUserId = process.env.IG_USER_ID;
  const pageId = process.env.FB_PAGE_ID;

  if (!igToken || !igUserId) {
    const err = new Error("Missing USER_ACCESS_TOKEN (or META_ACCESS_TOKEN) or IG_USER_ID in .env");
    err.httpStatus = 400;
    throw err;
  }

  const [since, until, weekStart, weekEndExclusive] = weekRangeUTC(safeWeekOffset);
  const weekEndInclusive = new Date(weekEndExclusive);
  weekEndInclusive.setUTCDate(weekEndInclusive.getUTCDate() - 1);

  const igBasicUrl =
    `https://graph.facebook.com/v21.0/${igUserId}` +
    `?fields=username,followers_count,media_count` +
    `&access_token=${encodeURIComponent(igToken)}`;

  const igInsightsMetrics = ["reach", "follower_count"];
  const igTotalValueMetrics = [
    "views",
    "accounts_engaged",
    "website_clicks",
    "profile_views",
    "profile_links_taps",
  ];

  const igInsightsBaseUrl =
    `https://graph.facebook.com/v21.0/${igUserId}/insights` +
    `?period=day&since=${since}&until=${until}` +
    `&access_token=${encodeURIComponent(igToken)}`;
  const igTotalValueBaseUrl =
    `https://graph.facebook.com/v21.0/${igUserId}/insights` +
    `?period=day&metric_type=total_value&since=${since}&until=${until}` +
    `&access_token=${encodeURIComponent(igToken)}`;

  const warnings = [];

  let igBasic = {};
  try {
    igBasic = await getJSON(igBasicUrl);
  } catch (e) {
    if (!isPermissionError(e)) throw e;
    warnings.push("Missing permission for Instagram basic fields (username/followers/media_count).");
  }

  let igInsights = { data: [] };
  try {
    igInsights = await fetchInsightsBestEffort(igInsightsBaseUrl, igInsightsMetrics);
  } catch (e) {
    if (!isPermissionError(e)) throw e;
    warnings.push("Missing permission for Instagram insights. Returning basic profile only.");
  }

  let igTotalValueInsights = {};
  try {
    igTotalValueInsights = await fetchInsightsTotalValueBestEffort(igTotalValueBaseUrl, igTotalValueMetrics);
  } catch (e) {
    if (!isPermissionError(e)) throw e;
    warnings.push("Missing permission for Instagram total-value insights.");
  }

  const igReach = sumMetricValues(igInsights.data, "reach");
  const igImpressions = igTotalValueInsights.views ?? null;
  const igInteractions = igTotalValueInsights.accounts_engaged ?? null;
  const igLinkClicks = igTotalValueInsights.website_clicks ?? igTotalValueInsights.profile_links_taps ?? null;
  const igDelta = followerDeltaForPeriod(igInsights.data);

  const igNewFollowers = igDelta != null ? Math.max(0, igDelta) : null;
  const igUnfollows = igDelta != null ? Math.max(0, -igDelta) : null;
  const igNetGrowth = igDelta;
  const igEngagementRate = pct(safeDiv(igInteractions, igReach));
  const igCTR = pct(safeDiv(igLinkClicks, igImpressions));

  let facebook = null;
  if (!pageId) {
    warnings.push("FB_PAGE_ID is not set. Skipping Facebook metrics.");
  } else if (!fbToken) {
    warnings.push("No Facebook token found. Set FB_ACCESS_TOKEN or META_ACCESS_TOKEN for Facebook metrics.");
  } else {
    const fetchFacebookWithToken = async (token) => {
      const fbBasicUrl =
        `https://graph.facebook.com/v21.0/${pageId}` +
        `?fields=name,fan_count` +
        `&access_token=${encodeURIComponent(token)}`;

      const fbInsightsMetrics = [
        "page_impressions_unique",
        "page_views_total",
        "page_post_engagements",
        "page_follows",
      ];

      const fbInsightsBaseUrl =
        `https://graph.facebook.com/v21.0/${pageId}/insights` +
        `?period=day&since=${since}&until=${until}` +
        `&access_token=${encodeURIComponent(token)}`;

      const fbBasic = await getJSON(fbBasicUrl);
      const out = {
        page_name: fbBasic.name ?? null,
        followers_now: fbBasic.fan_count ?? null,
        total_reach: null,
        total_impressions: null,
        content_interactions: null,
        engagement_rate_pct: null,
        link_clicks: null,
        ctr_pct: null,
        new_followers: null,
      };

      const fbInsights = await fetchInsightsBestEffort(fbInsightsBaseUrl, fbInsightsMetrics);
      const fbReach = firstAvailableMetric(fbInsights.data, ["page_impressions_unique"]);
      const fbImpressions = firstAvailableMetric(fbInsights.data, ["page_views_total"]);
      const fbInteractions = firstAvailableMetric(fbInsights.data, ["page_post_engagements"]);
      const fbLinkClicks = await fetchFacebookLinkClicksFromPosts(pageId, token, since, until);
      const fbNewFollowers = metricDeltaForPeriod(fbInsights.data, "page_follows");
      const fbEngagementRate = pct(safeDiv(fbInteractions, fbReach));
      const fbCTR = pct(safeDiv(fbLinkClicks, fbImpressions));

      out.total_reach = fbReach;
      out.total_impressions = fbImpressions;
      out.content_interactions = fbInteractions;
      out.engagement_rate_pct = fbEngagementRate;
      out.link_clicks = fbLinkClicks;
      out.ctr_pct = fbCTR;
      out.new_followers = fbNewFollowers;
      if (
        !Number.isFinite(fbReach) &&
        !Number.isFinite(fbImpressions) &&
        !Number.isFinite(fbInteractions) &&
        !Number.isFinite(fbLinkClicks)
      ) {
        warnings.push("Facebook insights returned empty data for this period/token.");
      }
      return out;
    };

    try {
      facebook = await fetchFacebookWithToken(fbToken);
    } catch (e) {
      if (isPageTokenRequiredError(e)) {
        try {
          const derivedPageToken = await getPageAccessTokenFromUserToken(userTokenForPageLookup, pageId);
          if (derivedPageToken) {
            facebook = await fetchFacebookWithToken(derivedPageToken);
            warnings.push("Facebook insights used a derived Page Access Token from your user token.");
          } else {
            warnings.push("Could not derive Page Access Token from user token. Set FB_ACCESS_TOKEN (page token).");
          }
        } catch (retryErr) {
          if (isPermissionError(retryErr) || isPageTokenRequiredError(retryErr)) {
            warnings.push("Facebook insights unavailable with current token/permissions. Showing basic page metrics only.");
          } else {
            throw retryErr;
          }
        }
      } else if (isPermissionError(e)) {
        warnings.push("Facebook insights unavailable with current token/permissions. Showing basic page metrics only.");
        try {
          const fbBasicUrl =
            `https://graph.facebook.com/v21.0/${pageId}` +
            `?fields=name,fan_count` +
            `&access_token=${encodeURIComponent(fbToken)}`;
          const fbBasic = await getJSON(fbBasicUrl);
          facebook = {
            page_name: fbBasic.name ?? null,
            followers_now: fbBasic.fan_count ?? null,
            total_reach: null,
            total_impressions: null,
            content_interactions: null,
            engagement_rate_pct: null,
            link_clicks: null,
            ctr_pct: null,
            new_followers: null,
          };
        } catch {
          // keep null if even basic fetch fails
        }
      } else {
        throw e;
      }
    }
  }

  const payload = {
    cached: false,
    updated_at: new Date().toISOString(),
    period: {
      type: "weekly",
      week_offset: safeWeekOffset,
      start_utc: weekStart.toISOString(),
      end_utc_inclusive: weekEndInclusive.toISOString(),
      label: `${weekStart.toISOString().slice(0, 10)} to ${weekEndInclusive.toISOString().slice(0, 10)} (Sun-Sat, UTC)`,
    },
    instagram: {
      username: igBasic.username ?? null,
      followers_now: igBasic.followers_count ?? null,
      media_count: igBasic.media_count ?? null,
      total_reach: igReach,
      total_impressions: igImpressions,
      content_interactions: igInteractions,
      engagement_rate_pct: igEngagementRate,
      link_clicks: igLinkClicks,
      ctr_pct: igCTR,
      new_followers: igNewFollowers,
      unfollows: igUnfollows,
      net_follower_growth: igNetGrowth,
    },
    facebook,
    warnings,
  };

  cache = { key: cacheKey, ts: now, data: payload };
  return payload;
}

function extractResponseText(data) {
  if (typeof data?.output_text === "string" && data.output_text.trim()) {
    return data.output_text.trim();
  }

  const chunks = [];
  for (const item of data?.output || []) {
    for (const content of item?.content || []) {
      if (content?.type === "output_text" && typeof content?.text === "string") {
        chunks.push(content.text);
      }
    }
  }
  return chunks.join("\n").trim();
}

function normalizeAiSummary(text) {
  if (typeof text !== "string") return "";

  const lines = text
    .replace(/\r/g, "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const cleaned = [];
  for (const line of lines) {
    const normalized = line.replace(/^[*\-•\d\.\)\s]+/, "").trim();
    if (!normalized) continue;
    if (/^recommended action:/i.test(normalized)) {
      cleaned.push(`Recommended action: ${normalized.replace(/^recommended action:\s*/i, "").trim()}`);
      continue;
    }
    cleaned.push(`- ${normalized}`);
  }

  const bullets = cleaned.filter((line) => line.startsWith("- ")).slice(0, 3);
  const action = cleaned.find((line) => line.startsWith("Recommended action:"));

  if (action) return [...bullets, action].join("\n");
  return bullets.join("\n");
}

async function generateAiWeeklySummary(currentSummary, previousSummary, respondCurrent, respondPrevious) {
  const analyticsPayload = {
    current_period: currentSummary?.period?.label || "Week A",
    previous_period: previousSummary?.period?.label || "Week B",
    instagram: {
      current: compactMetrics(currentSummary?.instagram, [
        "followers_now",
        "total_reach",
        "total_impressions",
        "content_interactions",
        "engagement_rate_pct",
        "link_clicks",
        "ctr_pct",
        "new_followers",
        "unfollows",
        "net_follower_growth",
      ]),
      previous: compactMetrics(previousSummary?.instagram, [
        "followers_now",
        "total_reach",
        "total_impressions",
        "content_interactions",
        "engagement_rate_pct",
        "link_clicks",
        "ctr_pct",
        "new_followers",
        "unfollows",
        "net_follower_growth",
      ]),
      deltas: {
        followers_now: delta(currentSummary?.instagram?.followers_now, previousSummary?.instagram?.followers_now),
        total_reach: delta(currentSummary?.instagram?.total_reach, previousSummary?.instagram?.total_reach),
        total_impressions: delta(currentSummary?.instagram?.total_impressions, previousSummary?.instagram?.total_impressions),
        content_interactions: delta(currentSummary?.instagram?.content_interactions, previousSummary?.instagram?.content_interactions),
        engagement_rate_pct: delta(currentSummary?.instagram?.engagement_rate_pct, previousSummary?.instagram?.engagement_rate_pct),
        link_clicks: delta(currentSummary?.instagram?.link_clicks, previousSummary?.instagram?.link_clicks),
        ctr_pct: delta(currentSummary?.instagram?.ctr_pct, previousSummary?.instagram?.ctr_pct),
        new_followers: delta(currentSummary?.instagram?.new_followers, previousSummary?.instagram?.new_followers),
        unfollows: delta(currentSummary?.instagram?.unfollows, previousSummary?.instagram?.unfollows),
        net_follower_growth: delta(currentSummary?.instagram?.net_follower_growth, previousSummary?.instagram?.net_follower_growth),
      },
    },
    facebook: {
      current: compactMetrics(currentSummary?.facebook, [
        "followers_now",
        "total_reach",
        "total_impressions",
        "content_interactions",
        "engagement_rate_pct",
        "link_clicks",
        "ctr_pct",
        "new_followers",
      ]),
      previous: compactMetrics(previousSummary?.facebook, [
        "followers_now",
        "total_reach",
        "total_impressions",
        "content_interactions",
        "engagement_rate_pct",
        "link_clicks",
        "ctr_pct",
        "new_followers",
      ]),
      deltas: {
        followers_now: delta(currentSummary?.facebook?.followers_now, previousSummary?.facebook?.followers_now),
        total_reach: delta(currentSummary?.facebook?.total_reach, previousSummary?.facebook?.total_reach),
        total_impressions: delta(currentSummary?.facebook?.total_impressions, previousSummary?.facebook?.total_impressions),
        content_interactions: delta(currentSummary?.facebook?.content_interactions, previousSummary?.facebook?.content_interactions),
        engagement_rate_pct: delta(currentSummary?.facebook?.engagement_rate_pct, previousSummary?.facebook?.engagement_rate_pct),
        link_clicks: delta(currentSummary?.facebook?.link_clicks, previousSummary?.facebook?.link_clicks),
        ctr_pct: delta(currentSummary?.facebook?.ctr_pct, previousSummary?.facebook?.ctr_pct),
        new_followers: delta(currentSummary?.facebook?.new_followers, previousSummary?.facebook?.new_followers),
      },
    },
    respondio: {
      current_people_contacted: safeNumber(respondCurrent?.people_contacted),
      previous_people_contacted: safeNumber(respondPrevious?.people_contacted),
      delta_people_contacted: delta(respondCurrent?.people_contacted, respondPrevious?.people_contacted),
    },
    warnings: [
      ...(Array.isArray(currentSummary?.warnings) ? currentSummary.warnings : []),
      ...(Array.isArray(previousSummary?.warnings) ? previousSummary.warnings : []),
    ],
  };

  const prompt = [
    "You summarize weekly social analytics using only the numbers in the JSON payload.",
    "Hard rules:",
    "1. Return exactly 3 bullet points and 1 final line starting with 'Recommended action:'.",
    "2. Each bullet must be a single short sentence.",
    "3. Every bullet must mention at least one explicit metric from the payload.",
    "4. If a metric is missing, ignore it.",
    "5. Do not guess causes, campaigns, ads, post formats, audience behavior, or strategy details not present in the payload.",
    "6. Do not mention reels, carousels, ads, trends, quality, authenticity, or content types unless those exact words are in the payload.",
    "7. Keep the recommendation conservative and based only on the largest visible change in the numbers.",
    "8. Output plain text only.",
    "",
    "Good example format:",
    "- Instagram reach fell from 420,000 to 335,000.",
    "- Facebook engagement rate dropped from 9.10% to 7.38%.",
    "- respond.io contacts increased from 41 to 56.",
    "Recommended action: Review the channel with the largest reach drop first.",
    "",
    "Analytics payload:",
    JSON.stringify(analyticsPayload, null, 2),
  ].join("\n");

  const openAiKey = process.env.OPENAI_API_KEY;
  if (openAiKey) {
    const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
    const controller = new AbortController();
    const timeoutMs = 12000;
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    let response;
    let data;
    try {
      response = await fetch("https://api.openai.com/v1/responses", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${openAiKey}`,
        },
        body: JSON.stringify({
          model,
          input: [
            {
              role: "system",
              content: [
                {
                  type: "input_text",
                  text: "You summarize weekly social analytics.",
                },
              ],
            },
            {
              role: "user",
              content: [
                {
                  type: "input_text",
                  text: prompt,
                },
              ],
            },
          ],
          max_output_tokens: 220,
        }),
        signal: controller.signal,
      });

      data = await response.json();
    } catch (e) {
      if (e?.name === "AbortError") {
        const err = new Error("AI summary timed out. Try again in a moment.");
        err.httpStatus = 504;
        throw err;
      }
      throw e;
    } finally {
      clearTimeout(timeoutId);
    }

    if (!response.ok) {
      const err = new Error(data?.error?.message || `OpenAI request failed with HTTP ${response.status}`);
      err.httpStatus = response.status;
      throw err;
    }

    const text = extractResponseText(data);
    if (!text) throw new Error("OpenAI returned an empty summary.");

    return { model, provider: "openai", text: normalizeAiSummary(text) };
  }

  const ollamaModel = process.env.OLLAMA_MODEL || "qwen2.5:7b";
  const controller = new AbortController();
  const timeoutMs = 20000;
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  let data;
  try {
    response = await fetch("http://127.0.0.1:11434/api/generate", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: ollamaModel,
        prompt,
        stream: false,
      }),
      signal: controller.signal,
    });

    data = await response.json();
  } catch (e) {
    if (e?.name === "AbortError") {
      const err = new Error("Ollama summary timed out. Try a smaller model or try again.");
      err.httpStatus = 504;
      throw err;
    }
    throw e;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    const msg = data?.error || `Ollama request failed with HTTP ${response.status}`;
    const err = new Error(msg);
    err.httpStatus = response.status;
    throw err;
  }

  const text = typeof data?.response === "string" ? data.response.trim() : "";
  if (!text) {
    const err = new Error(`Ollama model '${ollamaModel}' returned an empty summary.`);
    err.httpStatus = 502;
    throw err;
  }

  return { model: ollamaModel, provider: "ollama", text: normalizeAiSummary(text) };
}

function parseAiSummaryRequest(body) {
  return {
    currentSummary: body?.current_summary || null,
    previousSummary: body?.previous_summary || null,
    respondCurrent: body?.respond_current || null,
    respondPrevious: body?.respond_previous || null,
  };
}

app.get("/api/marketing/channels", (req, res) => {
  res.json({
    updated_at: new Date().toISOString(),
    channels: [
      {
        platform: "LinkedIn",
        handle: "icenteriraq",
        url: "https://www.linkedin.com/company/icenteriraq",
        followers: parseOptionalInt(process.env.LINKEDIN_FOLLOWERS),
      },
      {
        platform: "TikTok",
        handle: "icenter.iraq",
        url: "https://www.tiktok.com/@icenter.iraq",
        followers: parseOptionalInt(process.env.TIKTOK_FOLLOWERS),
      },
      {
        platform: "YouTube",
        handle: "icenter-iraq",
        url: "https://www.youtube.com/@icenter-iraq",
        followers: parseOptionalInt(process.env.YOUTUBE_SUBSCRIBERS),
      },
      {
        platform: "Instagram Channel",
        handle: "news & updates",
        url: "https://www.instagram.com/channel/AbZ4qYFv5-IGtzdO/",
        followers: parseOptionalInt(process.env.INSTAGRAM_CHANNEL_MEMBERS),
      },
      {
        platform: "Telegram",
        handle: "icenterar",
        url: "https://t.me/icenterar",
        followers: parseOptionalInt(process.env.TELEGRAM_SUBSCRIBERS),
      },
      {
        platform: "WhatsApp Channel",
        handle: "0029Vb6KGfUI7BeNDDHOlz3Y",
        url: "https://whatsapp.com/channel/0029Vb6KGfUI7BeNDDHOlz3Y",
        followers: parseOptionalInt(process.env.WHATSAPP_CHANNEL_FOLLOWERS),
      },
    ],
  });
});

app.get("/api/respondio/summary", async (req, res) => {
  const stats = await readRespondioStats();
  res.json({
    platform: "respond.io",
    ...stats,
  });
});

app.get("/api/respondio/weekly-contacts", async (req, res) => {
  try {
    const weekOffsetRaw = Number.parseInt(String(req.query.week_offset ?? "0"), 10);
    const weekOffset = Number.isFinite(weekOffsetRaw) && weekOffsetRaw >= 0 ? weekOffsetRaw : 0;
    res.json(await getRespondioWeeklyContacts(weekOffset));
  } catch (e) {
    res.status(e.httpStatus || 500).json({ error: e.message || "Failed to fetch respond.io weekly contacts" });
  }
});

app.post("/api/respondio/webhook", async (req, res) => {
  try {
    const expectedSecret = process.env.RESPONDIO_WEBHOOK_SECRET;
    if (expectedSecret) {
      const provided =
        req.headers["x-respondio-secret"] ||
        req.headers["x-webhook-secret"] ||
        req.query.secret;
      if (provided !== expectedSecret) {
        return res.status(401).json({ error: "Invalid webhook secret." });
      }
    }

    const now = new Date().toISOString();
    const stats = await readRespondioStats();
    stats.webhook_events += 1;
    stats.last_event_at = now;

    if (isRespondioMessageEvent(req.body)) {
      stats.messages_received += 1;
      stats.last_message_at = now;
    }

    await writeRespondioStats(stats);
    res.json({ ok: true, stats });
  } catch (e) {
    res.status(500).json({ error: e.message || "Failed to process respond.io webhook." });
  }
});

app.get("/api/meta/summary", async (req, res) => {
  try {
    const weekOffsetRaw = Number.parseInt(String(req.query.week_offset ?? "0"), 10);
    const weekOffset = Number.isFinite(weekOffsetRaw) && weekOffsetRaw >= 0 ? weekOffsetRaw : 0;
    res.json(await getMetaSummary(weekOffset));
  } catch (e) {
    const mapped = mapMetaError(e);
    if (mapped) return res.status(mapped.httpStatus).json(mapped.payload);
    res.status(e.httpStatus || 500).json({ error: e.message });
  }
});

app.get("/api/ai/summary", async (req, res) => {
  try {
    const weekOffsetARaw = Number.parseInt(String(req.query.week_offset_a ?? "0"), 10);
    const weekOffsetBRaw = Number.parseInt(String(req.query.week_offset_b ?? "1"), 10);
    const weekOffsetA = Number.isFinite(weekOffsetARaw) && weekOffsetARaw >= 0 ? weekOffsetARaw : 0;
    const weekOffsetB = Number.isFinite(weekOffsetBRaw) && weekOffsetBRaw >= 0 ? weekOffsetBRaw : 1;
    const cacheKey = `ai-summary:${weekOffsetA}:${weekOffsetB}`;
    const now = Date.now();

    if (aiSummaryCache.data && aiSummaryCache.key === cacheKey && now - aiSummaryCache.ts < AI_SUMMARY_CACHE_TTL_MS) {
      return res.json({ ...aiSummaryCache.data, cached: true });
    }

    const [currentSummary, previousSummary, respondCurrent, respondPrevious] = await Promise.all([
      getMetaSummary(weekOffsetA),
      getMetaSummary(weekOffsetB),
      getRespondioWeeklyContacts(weekOffsetA).catch(() => null),
      getRespondioWeeklyContacts(weekOffsetB).catch(() => null),
    ]);

    const ai = await generateAiWeeklySummary(
      currentSummary,
      previousSummary,
      respondCurrent,
      respondPrevious
    );

    const payload = {
      cached: false,
      updated_at: new Date().toISOString(),
      period_a: currentSummary?.period || null,
      period_b: previousSummary?.period || null,
      summary: ai.text,
      model: ai.model,
      provider: ai.provider,
    };

    aiSummaryCache = { key: cacheKey, ts: now, data: payload };
    res.json(payload);
  } catch (e) {
    res.status(e.httpStatus || 500).json({ error: e.message || "Failed to generate AI summary" });
  }
});

app.post("/api/ai/summary", async (req, res) => {
  try {
    const { currentSummary, previousSummary, respondCurrent, respondPrevious } = parseAiSummaryRequest(req.body);
    if (!currentSummary || !previousSummary) {
      return res.status(400).json({ error: "Missing current_summary or previous_summary." });
    }

    const ai = await generateAiWeeklySummary(
      currentSummary,
      previousSummary,
      respondCurrent,
      respondPrevious
    );

    res.json({
      cached: false,
      updated_at: new Date().toISOString(),
      period_a: currentSummary?.period || null,
      period_b: previousSummary?.period || null,
      summary: ai.text,
      model: ai.model,
      provider: ai.provider,
    });
  } catch (e) {
    res.status(e.httpStatus || 500).json({ error: e.message || "Failed to generate AI summary" });
  }
});

// Keep your old endpoints if you still use them elsewhere (optional)
app.get("/api/meta/instagram", async (req, res) => {
  res.status(410).json({ error: "Use /api/meta/summary instead." });
});
app.get("/api/meta/facebook-page", async (req, res) => {
  try {
    const pageId = process.env.FB_PAGE_ID;
    const token = process.env.FB_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN || process.env.USER_ACCESS_TOKEN;
    if (!pageId || !token) return res.status(400).json({ error: "Missing FB_PAGE_ID or FB_ACCESS_TOKEN (or META_ACCESS_TOKEN)" });

    const url =
      `https://graph.facebook.com/v21.0/${pageId}` +
      `?fields=name,fan_count` +
      `&access_token=${encodeURIComponent(token)}`;
    const data = await getJSON(url);

    res.json({
      platform: "facebook",
      page: data.name ?? null,
      followers: data.fan_count ?? null,
    });
  } catch (e) {
    const mapped = mapMetaError(e);
    if (mapped) return res.status(mapped.httpStatus).json(mapped.payload);
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/meta/video-insights", async (req, res) => {
  res.status(410).json({ error: "Video insights endpoint removed. It requires page-level access." });
});

app.listen(3000, () => console.log("Dashboard running on http://localhost:3000"));
