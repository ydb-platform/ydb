SELECT
    Data
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE'
    )
;
