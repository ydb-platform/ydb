/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
SELECT
    key
FROM (
    SELECT
        key,
        value
    FROM plato.Input
)
    TABLESAMPLE BERNOULLI (33)
ORDER BY
    key;
