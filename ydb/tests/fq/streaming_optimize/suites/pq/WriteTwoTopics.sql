INSERT INTO pq.test_topic_output
SELECT
    Data
FROM
    pq.test_topic_input WITH (
        STREAMING = 'TRUE'
    )
;

INSERT INTO pq.test_topic_output2
SELECT
    Data
FROM
    pq.test_topic_input2 WITH (
        STREAMING = 'TRUE'
    )
;
