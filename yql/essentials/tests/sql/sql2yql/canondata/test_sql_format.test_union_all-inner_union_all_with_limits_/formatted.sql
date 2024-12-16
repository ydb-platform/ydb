USE plato;

SELECT
    key,
    value
FROM (
    (
        SELECT
            *
        FROM
            Input
        LIMIT 3
    )
    UNION ALL
    (
        SELECT
            *
        FROM
            Input
        LIMIT 2
    )
)
WHERE
    key < "100"
;

SELECT
    key,
    value
FROM (
    (
        SELECT
            *
        FROM
            Input
        LIMIT 3
    )
    UNION ALL
    SELECT
        *
    FROM
        Input
)
WHERE
    key < "200"
;
