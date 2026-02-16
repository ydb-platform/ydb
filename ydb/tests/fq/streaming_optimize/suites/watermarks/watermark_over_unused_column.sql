PRAGMA dq.WatermarksMode="default";
PRAGMA pq.Consumer="test_client";

SELECT
    payload
FROM pq.test_topic_input
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        ts Timestamp,
        payload String,
    ),
    WATERMARK = ts - Interval("PT5S")
);
