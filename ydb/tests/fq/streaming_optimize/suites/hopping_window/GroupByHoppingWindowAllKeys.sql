$input = (
    SELECT
        *
    FROM
        pq.test_topic_input WITH (
            FORMAT = json_each_row,
            SCHEMA (t String, k String, v Int64),
            STREAMING = 'TRUE'
        )
);

$output = (
    SELECT
        k,
        Sum(v) AS sum
    FROM
        $input
    GROUP BY
        k,
        HoppingWindow(
            CAST(t AS Timestamp), 'PT0.005S', 'PT0.01S',
            'PT1H' AS TimeLimit,
            12345678 AS SizeLimit,
            'drop' AS EarlyPolicy,
            'adjust' AS LatePolicy
        )
);

INSERT INTO pq.test_topic_output
SELECT
    Yson::SerializeText(Yson::From(TableRow()))
FROM
    $output
;
