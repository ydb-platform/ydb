$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|k: 1, v: 1|>,
            <|k: 1, v: 2|>,
            <|k: 1, v: 3|>,
            <|k: 1, v: 4|>,
            <|k: 1, v: 5|>,
        ])
);

SELECT
    *
FROM
    $input
    FLATTEN BY (
        ListFromRange(1, 3) AS x
    )
;

SELECT
    *
FROM
    $input
    FLATTEN BY (
        ListFromRange(
            1, (
                SELECT
                    3
            )
        ) AS x
    )
;

SELECT
    *
FROM
    $input
    FLATTEN BY (
        ListFromRange(
            1, (
                SELECT
                    Avg(v)
                FROM
                    $input
            )
        ) AS x
    )
;

SELECT
    *
FROM
    $input
    FLATTEN BY (
        ListFromRange(
            1, (
                SELECT
                    Avg(v)
                FROM
                    $input
                WHERE
                    v == 3
            )
        ) AS x
    )
;
