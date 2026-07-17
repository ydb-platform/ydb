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
        percentile(v, 0.75) AS p75,
        percentile(v, 0.9) AS p90
    FROM
        $input
    GROUP BY
        k,
        HOP (CAST(t AS Timestamp), 'PT0.005S', 'PT0.01S', 'PT0.01S')
);

INSERT INTO pq.test_topic_output
SELECT
    Yson::SerializeText(Yson::From(TableRow()))
FROM
    $output
;
