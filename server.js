import express from "express";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.static("public"));

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
    const requiresPageToken =
      typeof metaErr.message === "string" && metaErr.message.includes("Page Access Token");

    if (requiresPageToken) {
      return {
        httpStatus: 401,
        payload: {
          error: metaErr.message,
          token_expired: false,
          token_invalid: true,
          meta_error_code: metaErr.code,
          meta_error_subcode: metaErr.error_subcode ?? null,
          action:
            "Use a Facebook Page Access Token for META_ACCESS_TOKEN (derived from a user with access to the page), then restart the server.",
        },
      };
    }

    return {
      httpStatus: 401,
      payload: {
        error: metaErr.message || "Meta access token is invalid or expired.",
        token_expired: isExpired,
        token_invalid: !isExpired,
        meta_error_code: metaErr.code,
        meta_error_subcode: metaErr.error_subcode ?? null,
        action: isExpired
          ? "Generate a new long-lived Meta token and replace META_ACCESS_TOKEN in .env, then restart the server."
          : "Replace META_ACCESS_TOKEN with a valid token that has access to the configured page/IG account, then restart the server.",
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

function unixSeconds(d) {
  return Math.floor(d.getTime() / 1000);
}

function dayRangeUTC(daysBackStart, daysBackEnd) {
  // returns [since, until] in unix seconds (UTC midnight boundaries)
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())); // today 00:00 UTC
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - daysBackStart);
  const until = new Date(end);
  until.setUTCDate(until.getUTCDate() - daysBackEnd);

  return [unixSeconds(start), unixSeconds(until)];
}

function pickLatestValue(insightsData, metricName) {
  const item = (insightsData || []).find((m) => m.name === metricName);
  if (!item || !item.values || item.values.length === 0) return null;

  // values are usually [{ value, end_time }, ...]
  const last = item.values[item.values.length - 1];
  return typeof last?.value === "number" ? last.value : null;
}

function pct(n) {
  return Number.isFinite(n) ? Math.round(n * 10000) / 100 : null; // 2 decimals
}

function safeDiv(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  return a / b;
}

// ---- Simple server-side cache to avoid rate limiting
const CACHE_TTL_MS = 15000; // Meta call at most every 15s
let cache = { ts: 0, data: null };

app.get("/api/meta/summary", async (req, res) => {
  try {
    const now = Date.now();
    if (cache.data && now - cache.ts < CACHE_TTL_MS) {
      return res.json({ ...cache.data, cached: true });
    }

    const token = process.env.META_ACCESS_TOKEN;
    const igUserId = process.env.IG_USER_ID;
    const pageId = process.env.FB_PAGE_ID;

    if (!token || !igUserId || !pageId) {
      return res.status(400).json({
        error: "Missing META_ACCESS_TOKEN, IG_USER_ID, or FB_PAGE_ID in .env",
      });
    }

    // We’ll fetch “yesterday” and “day before yesterday” to compute deltas.
    // since = 2 days ago 00:00 UTC, until = today 00:00 UTC
    const [since, until] = dayRangeUTC(2, 0);

    // ---- Instagram account basic
    const igBasicUrl =
      `https://graph.facebook.com/v21.0/${igUserId}` +
      `?fields=username,followers_count,media_count` +
      `&access_token=${encodeURIComponent(token)}`;

    // ---- Instagram insights (daily)
    // Metrics we’ll try:
    // reach, impressions, accounts_engaged, website_clicks, profile_views, follower_count
    const igInsightsMetrics = [
      "reach",
      "impressions",
      "accounts_engaged",
      "website_clicks",
      "profile_views",
      "follower_count",
    ];

    const igInsightsBaseUrl =
      `https://graph.facebook.com/v21.0/${igUserId}/insights` +
      `?period=day&since=${since}&until=${until}` +
      `&access_token=${encodeURIComponent(token)}`;

    // ---- Facebook Page basic
    const fbBasicUrl =
      `https://graph.facebook.com/v21.0/${pageId}` +
      `?fields=name,fan_count` +
      `&access_token=${encodeURIComponent(token)}`;

    // ---- Facebook Page insights (daily)
    // Reach-ish: page_impressions_unique
    // Impressions: page_impressions
    // Interactions: page_engaged_users
    // Link clicks (best-effort): page_consumptions (this is “clicks on any content”)
    // Some pages have different availability depending on permissions/rollouts.
    const fbInsightsMetrics = [
      "page_impressions_unique",
      "page_impressions",
      "page_engaged_users",
      "page_consumptions",
    ];

    const fbInsightsBaseUrl =
      `https://graph.facebook.com/v21.0/${pageId}/insights` +
      `?period=day&since=${since}&until=${until}` +
      `&access_token=${encodeURIComponent(token)}`;

    const [igBasic, igInsights, fbBasic, fbInsights] = await Promise.all([
      getJSON(igBasicUrl),
      fetchInsightsBestEffort(igInsightsBaseUrl, igInsightsMetrics),
      getJSON(fbBasicUrl),
      fetchInsightsBestEffort(fbInsightsBaseUrl, fbInsightsMetrics),
    ]);

    // ---- Instagram metrics
    const igReach = pickLatestValue(igInsights.data, "reach");
    const igImpressions = pickLatestValue(igInsights.data, "impressions");
    const igInteractions = pickLatestValue(igInsights.data, "accounts_engaged"); // “content interactions” proxy
    const igLinkClicks = pickLatestValue(igInsights.data, "website_clicks");
    const igFollowerCountDaily = igInsights.data?.find((m) => m.name === "follower_count")?.values || [];

    const igYesterdayFollowers =
      igFollowerCountDaily.length >= 2 ? igFollowerCountDaily[igFollowerCountDaily.length - 2]?.value : null;
    const igTodayFollowers =
      igFollowerCountDaily.length >= 1 ? igFollowerCountDaily[igFollowerCountDaily.length - 1]?.value : null;

    const igDelta = (Number.isFinite(igTodayFollowers) && Number.isFinite(igYesterdayFollowers))
      ? igTodayFollowers - igYesterdayFollowers
      : null;

    const igNewFollowers = igDelta != null ? Math.max(0, igDelta) : null;
    const igUnfollows = igDelta != null ? Math.max(0, -igDelta) : null;
    const igNetGrowth = igDelta;

    const igEngagementRate = pct(safeDiv(igInteractions, igImpressions)); // interactions / impressions
    const igCTR = pct(safeDiv(igLinkClicks, igImpressions)); // clicks / impressions

    // ---- Facebook metrics
    const fbReach = pickLatestValue(fbInsights.data, "page_impressions_unique");
    const fbImpressions = pickLatestValue(fbInsights.data, "page_impressions");
    const fbInteractions = pickLatestValue(fbInsights.data, "page_engaged_users");
    const fbLinkClicks = pickLatestValue(fbInsights.data, "page_consumptions"); // best-effort “clicks”
    const fbEngagementRate = pct(safeDiv(fbInteractions, fbImpressions));
    const fbCTR = pct(safeDiv(fbLinkClicks, fbImpressions));

    // “New Followers” for Facebook: use fan_count delta from insights if available is messy,
    // so we do the same “delta from yesterday” approach by querying fan_count now vs storing history.
    // For now we return only current fan_count and leave “new followers” null unless you want persistence.
    const fbNewFollowers = null;

    const payload = {
      cached: false,
      updated_at: new Date().toISOString(),

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

      facebook: {
        page_name: fbBasic.name ?? null,
        followers_now: fbBasic.fan_count ?? null,

        total_reach: fbReach,
        total_impressions: fbImpressions,
        content_interactions: fbInteractions,
        engagement_rate_pct: fbEngagementRate, // %
        link_clicks: fbLinkClicks,
        ctr_pct: fbCTR, // %

        new_followers: fbNewFollowers, // null unless you want persistence
      },
    };

    cache = { ts: now, data: payload };
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
  res.status(410).json({ error: "Use /api/meta/summary instead." });
});

app.get("/api/meta/video-insights", async (req, res) => {
  try {
    const videoId = req.query.video_id;
    const token = process.env.META_ACCESS_TOKEN;
    if (!videoId) return res.status(400).json({ error: "Provide ?video_id=..." });
    if (!token) return res.status(400).json({ error: "Missing META_ACCESS_TOKEN" });

    const metrics = (req.query.metrics || "total_video_impressions,total_video_views").split(",");

    const url =
      `https://graph.facebook.com/v21.0/${videoId}/video_insights` +
      `?metric=${encodeURIComponent(metrics.join(","))}` +
      `&access_token=${encodeURIComponent(token)}`;

    const data = await getJSON(url);
    res.json({ platform: "meta", video_id: videoId, insights: data.data ?? data });
  } catch (e) {
    const mapped = mapMetaError(e);
    if (mapped) return res.status(mapped.httpStatus).json(mapped.payload);
    res.status(500).json({ error: e.message });
  }
});

app.listen(3000, () => console.log("Dashboard running on http://localhost:3000"));
