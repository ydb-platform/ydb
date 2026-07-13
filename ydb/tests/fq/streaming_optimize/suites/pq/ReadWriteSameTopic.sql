INSERT INTO pq.test_topic_input
SELECT
    random_column_name
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE',
        format = raw,
        SCHEMA (random_column_name STRING NOT NULL)
    )
;
