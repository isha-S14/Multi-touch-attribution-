Channel logic should be added AFTER sessionization and BEFORE attribution modeling.

 ### Why Channels Matter in Analytics

## Channels help answer:

Which marketing efforts work?

Where should we spend money?

How do customers move through the funnel?

## Without channels:
❌ You only know what users did
✔ With channels: you know why they came

## Data Pipeline 
raw_events
   ↓
clean_events
   ↓
session_events
   ↓
user_sessions   ← behavioral aggregation
   ↓
session_channels  ← CHANNEL LOGIC GOES HERE
   ↓
user_journeys
   ↓
attribution_results
Channel logic lives in session_channels

# sql query
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
    'behavior_inference' AS attribution_rule
FROM user_sessions;
