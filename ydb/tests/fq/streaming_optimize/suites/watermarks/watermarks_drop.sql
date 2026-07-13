SELECT
    *
FROM
    pq.test_topic_input WITH (
        FORMAT = json_each_row,
        SCHEMA (t String, k String, v Int64),
        WATERMARK = CAST(t AS Timestamp) - Interval('PT5S'),
        WATERMARK_GRANULARITY = 'PT2S',
        WATERMARK_IDLE_TIMEOUT = 'PT3S',
        STREAMING = 'TRUE'
    )
;
