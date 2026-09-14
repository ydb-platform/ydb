INSERT INTO pq.test_topic_output
SELECT
    value
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE',
        FORMAT = 'json_each_row',
        SCHEMA (value String NOT NULL, color String)
    )
;
