SELECT
    key
FROM (
    SELECT
        key,
        subkey
    FROM (
        SELECT
            1 AS key,
            'foo' AS subkey
        UNION ALL
        SELECT
            2 AS key,
            'bar' AS subkey
        UNION ALL
        SELECT
            3 AS key,
            123 AS subkey
    )
)
ORDER BY
    key
;

SELECT
    key
FROM (
    SELECT
        *
    FROM (
        SELECT
            4 AS key,
            'baz' AS subkey
        UNION ALL
        SELECT
            5 AS key,
            'goo' AS subkey
        UNION ALL
        SELECT
            6 AS key,
            456 AS subkey
    )
)
ORDER BY
    key
;
