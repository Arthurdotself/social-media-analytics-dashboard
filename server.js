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
    const cacheKey = `summary:week_offset=${weekOffset}`;
    const now = Date.now();
    if (cache.data && cache.key === cacheKey && now - cache.ts < CACHE_TTL_MS) {
      return res.json({ ...cache.data, cached: true });
    }

    const igToken = process.env.USER_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN;
    const fbToken = process.env.FB_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN || process.env.USER_ACCESS_TOKEN;
    const userTokenForPageLookup = process.env.USER_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN;
    const igUserId = process.env.IG_USER_ID;
    const pageId = process.env.FB_PAGE_ID;

    if (!igToken || !igUserId) {
      return res.status(400).json({
        error: "Missing USER_ACCESS_TOKEN (or META_ACCESS_TOKEN) or IG_USER_ID in .env",
      });
    }

    const [since, until, weekStart, weekEndExclusive] = weekRangeUTC(weekOffset);
    const weekEndInclusive = new Date(weekEndExclusive);
    weekEndInclusive.setUTCDate(weekEndInclusive.getUTCDate() - 1);

    // ---- Instagram account basic
    const igBasicUrl =
      `https://graph.facebook.com/v21.0/${igUserId}` +
      `?fields=username,followers_count,media_count` +
      `&access_token=${encodeURIComponent(igToken)}`;

    // ---- Instagram insights (daily values)
    // follower_count is kept here for net-growth calculation in the selected week.
    const igInsightsMetrics = [
      "reach",
      "follower_count",
    ];
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

    // ---- Instagram metrics (weekly sums / deltas for the selected Sunday..Saturday period)
    const igReach = sumMetricValues(igInsights.data, "reach");
    const igImpressions = igTotalValueInsights.views ?? null;
    const igInteractions = igTotalValueInsights.accounts_engaged ?? null;
    const igLinkClicks = igTotalValueInsights.website_clicks ?? igTotalValueInsights.profile_links_taps ?? null;
    const igDelta = followerDeltaForPeriod(igInsights.data);

    const igNewFollowers = igDelta != null ? Math.max(0, igDelta) : null;
    const igUnfollows = igDelta != null ? Math.max(0, -igDelta) : null;
    const igNetGrowth = igDelta;

    const igEngagementRate = pct(safeDiv(igInteractions, igReach)); // interactions / reach
    const igCTR = pct(safeDiv(igLinkClicks, igImpressions)); // clicks / impressions

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
          "page_total_actions",
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
        const fbLinkClicks = firstAvailableMetric(fbInsights.data, ["page_total_actions"]);
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
        week_offset: weekOffset,
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
        engagement_rate_pct: igEngagementRate, // %
        link_clicks: igLinkClicks,
        ctr_pct: igCTR, // %

        new_followers: igNewFollowers,
        unfollows: igUnfollows,
        net_follower_growth: igNetGrowth,
      },
      facebook,
      warnings,
    };

    cache = { key: cacheKey, ts: now, data: payload };
    res.json(payload);
  } catch (e) {
    const mapped = mapMetaError(e);
    if (mapped) return res.status(mapped.httpStatus).json(mapped.payload);
    res.status(500).json({ error: e.message });
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
