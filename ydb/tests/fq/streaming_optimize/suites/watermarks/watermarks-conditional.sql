PRAGMA dq.WatermarksMode="default";
PRAGMA pq.Consumer="test_client";

SELECT
    *
FROM pq.test_topic_input
WITH(
    FORMAT=json_each_row,
    SCHEMA(
        ts String,
        kind String
    ),
    WATERMARK = (CAST(ts AS Timestamp) - (CASE WHEN kind = "important" THEN Interval("PT5S") ELSE Interval("PT1M") END)),
    WATERMARK_GRANULARITY="PT2S",
    WATERMARK_IDLE_TIMEOUT="PT3S"
);
