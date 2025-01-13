/* postgres can not */
SELECT
    key,
    row_number() OVER w
FROM (
    SELECT
        'a' AS key,
        'z' AS value
)
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            value
    )
;
