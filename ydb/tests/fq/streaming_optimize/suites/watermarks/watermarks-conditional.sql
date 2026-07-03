PRAGMA dq.WatermarksMode="default";
PRAGMA pq.Consumer="test_client";

SELECT
    *
FROM pq.test_topic_input
WITH(
    FORMAT=json_each_row,
    SCHEMA(
        ts String,
        ts_imp String,
        kind String
    ),
    WATERMARK = (CASE WHEN kind = "important" THEN CAST(ts_imp AS Timestamp) ELSE CAST(ts AS Timestamp) END) - Interval("PT5S"),
    WATERMARK_GRANULARITY="PT2S",
    WATERMARK_IDLE_TIMEOUT="PT3S"
);
