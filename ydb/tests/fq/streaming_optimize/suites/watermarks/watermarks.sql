PRAGMA dq.WatermarksMode="default";
PRAGMA dq.WatermarksGranularityMs="4";
PRAGMA dq.WatermarksIdleTimeoutMs="5";
PRAGMA dq.WatermarksLateArrivalDelayMs="6";
PRAGMA dq.WatermarksEnableIdlePartitions="true";
PRAGMA pq.Consumer="test_client";

SELECT
    *
FROM pq.test_topic_input
WITH(
    FORMAT=json_each_row,
    SCHEMA(
        ts Timestamp NOT NULL
    ),
    WATERMARK = ts - Interval("PT5S"),
    WATERMARK_GRANULARITY="PT2S",
    WATERMARK_IDLE_TIMEOUT="PT3S"
);
