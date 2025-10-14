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
    (
        SELECT
            3
    ) AS x
;

SELECT
    (
        SELECT
            Avg(v)
        FROM
            $input
    ) AS x
;

SELECT
    (
        SELECT
            Avg(v)
        FROM
            $input
        WHERE
            v == 3
    ) AS x
;
