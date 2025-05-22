$p = (
    SELECT
        user,
        HOP_START() AS ts,
        SUM(payload) AS payload
    FROM (
        SELECT
            'foo' AS user,
            CAST(1 AS Timestamp) AS ts,
            10 AS payload
    )
    GROUP BY
        HOP (ts, 'PT10S', 'PT10S', 'PT10S'),
        user
);

$p = (
    PROCESS $p
);

SELECT
    FormatType(TypeOf($p))
; -- no MultiHoppingCore comp node
