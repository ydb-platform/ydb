/* syntax version 1 */
/* postgres can not */
SELECT
    key,
    min(AsTuple(subkey, value)) AS min,
    max(AsTuple(subkey, value)) AS max
FROM (
    SELECT
        key,
        (
            CASE
                WHEN length(subkey) != 0 THEN subkey
                ELSE NULL
            END
        ) AS subkey,
        (
            CASE
                WHEN length(value) != 0 THEN value
                ELSE NULL
            END
        ) AS value
    FROM
        plato.Input
)
GROUP BY
    key
ORDER BY
    key
;
