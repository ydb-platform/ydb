/* postgres can not */
$input = (
    SELECT
        CAST(key AS int32) / 100 AS key_hundred,
        CAST(key AS int32) AS key,
        CAST(subkey AS int32) AS subkey,
        value
    FROM plato.Input
);

SELECT
    key_hundred,
    key,
    FIRST_VALUE(CAST(subkey AS uint32)) RESPECT NULLS OVER w1 AS first_res_null,
    FIRST_VALUE(CAST(subkey AS uint32)) IGNORE NULLS OVER w1 AS first_esc_null,
    LAST_VALUE(CAST(subkey AS uint32)) OVER w1 AS last_res_null,
    LAST_VALUE(CAST(subkey AS uint32)) IGNORE NULLS OVER w1 AS last_esc_null,
    subkey
FROM $input
WINDOW
    w1 AS (
        PARTITION BY
            key_hundred
        ORDER BY
            key
    )
ORDER BY
    key_hundred,
    key,
    subkey;
