$input = (
    SELECT
        t,
        k,
        v,
        CASE
            WHEN v == 0 THEN CAST(t AS Timestamp)
            ELSE CAST(k AS Timestamp)
        END AS event_time
    FROM
        pq.test_topic_input WITH (
        FORMAT = json_each_row,
        SCHEMA (t String, k String, v Int64),
        STREAMING = 'TRUE'
    )
);

SELECT
    *
FROM
    $input WITH (
        WATERMARK = event_time - Interval('PT5S'),
        WATERMARK_GRANULARITY = 'PT2S',
        WATERMARK_IDLE_TIMEOUT = 'PT3S'
    )
;
