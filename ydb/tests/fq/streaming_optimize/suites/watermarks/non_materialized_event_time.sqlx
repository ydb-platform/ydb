SELECT
    *
FROM
    pq.test_topic_input WITH (
        FORMAT = json_each_row,
        SCHEMA (t String, k String, v Int64),
        WATERMARK = (
            CASE
                WHEN v == 0 THEN CAST(t AS Timestamp)
                ELSE CAST(k AS Timestamp)
            END
        ) - Interval('PT5S'),
        WATERMARK_GRANULARITY = 'PT2S',
        WATERMARK_IDLE_TIMEOUT = 'PT3S',
        STREAMING = 'TRUE'
    )
;
