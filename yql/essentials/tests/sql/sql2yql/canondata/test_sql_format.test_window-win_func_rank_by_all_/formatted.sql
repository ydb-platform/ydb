/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableAnsiRankForNullableKeys;

$input = (
    SELECT
        CAST(key AS int32) ?? 0 AS key,
        CAST(subkey AS int32) AS subkey,
        value
    FROM Input
);

SELECT
    rank(key) OVER w1 AS rank_key,
    dense_rank(key) OVER w1 AS dense_rank_key,
    key
FROM $input
WINDOW
    w1 AS (
        ORDER BY
            key
    );
