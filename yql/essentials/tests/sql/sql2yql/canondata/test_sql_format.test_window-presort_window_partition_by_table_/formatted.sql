/* postgres can not */
USE plato;

SELECT
    key,
    row_number() OVER w
FROM (
    SELECT
        AsList(key) AS key,
        value
    FROM Input
)
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            value
    )
ORDER BY
    key;
