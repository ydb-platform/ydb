/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 8 */
SELECT
    *
FROM (
    SELECT
        key
    FROM plato.Input
    WHERE subkey != "1"
)
    TABLESAMPLE BERNOULLI (44)
WHERE key > "50";
