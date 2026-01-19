/* STEP 0: CLEAN DATA (STAGING) */

CREATE TABLE clean_events AS
SELECT *
FROM raw_events
WHERE event_type IN ('view','cart','purchase')
  AND event_time BETWEEN '2019-01-01' AND CURRENT_TIMESTAMP;


/* Remove invalid purchases: */

DELETE FROM clean_events
WHERE event_type = 'purchase'
  AND (price IS NULL OR price <= 0);

/* STEP 1: SESSIONIZATION (30-MIN RULE */
*INSERT INTO session_events
SELECT
    event_id,
    user_id,
    SUM(new_session) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS session_id,
    event_time,
    event_type,
    product_id,
    category_code,
    price
FROM (
    SELECT *,
           CASE
               WHEN LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
                 OR event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)
                    > INTERVAL '30 minutes'
               THEN 1 ELSE 0
           END AS new_session
    FROM clean_events
) t;

/* STEP 2: SESSION-LEVEL METRICS */

INSERT INTO user_sessions
SELECT
    user_id,
    session_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end,
    COUNT(*) FILTER (WHERE event_type='view') AS views,
    COUNT(*) FILTER (WHERE event_type='cart') AS carts,
    COUNT(*) FILTER (WHERE event_type='purchase') AS purchases,
    COALESCE(SUM(price) FILTER (WHERE event_type='purchase'),0) AS revenue
FROM session_events
GROUP BY user_id, session_id;

/* STEP 3: ATTRIBUTION LOGIC */

INSERT INTO session_channels
SELECT
    user_id,
    session_id,
    CASE
        WHEN session_id = 1 THEN 'Paid Search'
        WHEN views >= 5 AND carts = 0 THEN 'Organic'
        WHEN carts > 0 AND purchases = 0 THEN 'Retargeting'
        WHEN purchases > 0 AND carts > 0 THEN 'Email'
        ELSE 'Direct'
    END AS channel,
    'behavior_based' AS attribution_rule
FROM user_sessions;

/* STEP 4: USER JOURNEYS */

INSERT INTO user_journeys
SELECT
    us.user_id,
    STRING_AGG(sc.channel, ' → ' ORDER BY us.session_id) AS journey,
    COUNT(*) AS total_sessions,
    MAX(us.purchases > 0) AS converted,
    SUM(us.revenue) AS total_revenue
FROM user_sessions us
JOIN session_channels sc
  ON us.user_id = sc.user_id
 AND us.session_id = sc.session_id
GROUP BY us.user_id;

/* STEP 5: ATTRIBUTION MODELS */

/* Last-Touch */

INSERT INTO attribution_results
SELECT
    'last_touch',
    sc.channel,
    SUM(us.revenue)
FROM user_sessions us
JOIN session_channels sc
  ON us.user_id=sc.user_id AND us.session_id=sc.session_id
WHERE us.purchases > 0
GROUP BY sc.channel;

/* First-Touch */

INSERT INTO attribution_results
SELECT
    'first_touch',
    sc.channel,
    SUM(uj.total_revenue)
FROM (
    SELECT DISTINCT ON (user_id) user_id, session_id
    FROM user_sessions
    ORDER BY user_id, session_id
) ft
JOIN session_channels sc USING (user_id, session_id)
JOIN user_journeys uj USING (user_id)
GROUP BY sc.channel;

/* Linear Attribution */
INSERT INTO attribution_results
SELECT
    'linear',
    sc.channel,
    SUM(uj.total_revenue / uj.total_sessions)
FROM session_channels sc
JOIN user_journeys uj USING (user_id)
WHERE uj.converted = TRUE
GROUP BY sc.channel;

/* Time-Decay Attribution */

INSERT INTO attribution_results
SELECT
    'time_decay',
    sc.channel,
    SUM(uj.total_revenue * (1.0 / rn))
FROM (
    SELECT
        user_id,
        session_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY session_start DESC
        ) AS rn
    FROM user_sessions
) ranked
JOIN session_channels sc USING (user_id, session_id)
JOIN user_journeys uj USING (user_id)
WHERE uj.converted = TRUE
GROUP BY sc.channel;

