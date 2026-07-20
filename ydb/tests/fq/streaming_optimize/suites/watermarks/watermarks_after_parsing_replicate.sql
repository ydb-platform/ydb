$parsed_input = (
    SELECT
        Yson::ConvertTo(Yson::ParseJson(line), Struct<t: String, k: String, v: Int64>) AS row
    FROM
        pq.test_topic_input WITH STREAMING = 'TRUE'
        FLATTEN LIST BY (
            String::SplitToList(Data, '.') AS line
        )
    WHERE
        line != ''
);

$parsed_input = (
    SELECT
        t,
        k,
        v
    FROM
        $parsed_input
        FLATTEN COLUMNS
);

$input = (
    SELECT
        t,
        k,
        v,
        CAST(t AS Timestamp) AS event_time
    FROM
        $parsed_input WITH (
            WATERMARK = CAST(t AS Timestamp) - Interval('PT5S')
        ) AS input
);

$output = (
    SELECT
        k,
        Sum(v) AS sum
    FROM
        $input
    GROUP BY
        k,
        HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
);

INSERT INTO pq.test_topic_output
SELECT
    Yson::SerializeText(Yson::From(TableRow()))
FROM
    $output
;

INSERT INTO pq.test_topic_output2
SELECT
    Yson::SerializeText(Yson::From(TableRow()))
FROM
    $parsed_input
;
