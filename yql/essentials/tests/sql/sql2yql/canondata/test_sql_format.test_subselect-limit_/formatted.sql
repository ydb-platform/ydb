$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|k: 1, v: 1|>,
            <|k: 2, v: 2|>,
            <|k: 3, v: 3|>,
            <|k: 4, v: 4|>,
            <|k: 5, v: 5|>,
        ])
);

SELECT
    *
FROM
    $input
LIMIT CAST(3 AS UInt64);

SELECT
    *
FROM
    $input
LIMIT CAST(
    (
        SELECT
            3
    ) AS UInt64
);

SELECT
    *
FROM
    $input
LIMIT CAST(
    (
        SELECT
            Avg(v)
        FROM
            $input
    ) AS UInt64
);

SELECT
    *
FROM
    $input
LIMIT CAST(
    (
        SELECT
            Avg(v)
        FROM
            $input
        WHERE
            v == 3
    ) AS UInt64
);
