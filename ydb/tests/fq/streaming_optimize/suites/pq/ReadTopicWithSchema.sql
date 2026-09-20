SELECT
    *
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE',
        FORMAT = 'json_each_row',
        SCHEMA (value String, color String)
    )
;
