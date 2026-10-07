INSERT INTO pq.test_topic_output
SELECT
    Data
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE'
    )
;
