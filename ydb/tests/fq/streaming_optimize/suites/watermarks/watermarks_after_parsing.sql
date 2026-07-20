$input = (
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

$input = (
    SELECT
        t,
        k,
        v
    FROM
        $input
        FLATTEN COLUMNS
);

SELECT
    t,
    k,
    v,
    CAST(t AS Timestamp) AS event_time
FROM
    $input WITH (
        WATERMARK = CAST(t AS Timestamp) - Interval('PT5S')
    ) AS input
;
