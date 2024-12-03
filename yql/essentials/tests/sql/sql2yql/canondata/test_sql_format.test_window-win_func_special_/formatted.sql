/* postgres can not */
$input = (
    SELECT
        CAST(key AS int32) AS key,
        CAST(subkey AS int32) AS subkey,
        value
    FROM plato.Input
);

SELECT
    key,
    (key - lag(key, 1) OVER w) AS key_diff,
    (subkey - lag(subkey, 1) OVER w) AS subkey_diff,
    row_number() OVER w AS row,
    value
FROM $input
WINDOW
    w AS (
        ORDER BY
            key,
            subkey,
            value
    );
