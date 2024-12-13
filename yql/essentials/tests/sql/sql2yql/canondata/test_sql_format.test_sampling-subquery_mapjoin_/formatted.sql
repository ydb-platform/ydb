/* syntax version 1 */
/* postgres can not */
/* hybridfile can not YQL-17764 */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.MapJoinLimit = "1m";

SELECT
    *
FROM (
    SELECT
        *
    FROM
        plato.Input AS a
    INNER JOIN
        plato.Input AS b
    ON
        a.key == b.key
)
TABLESAMPLE BERNOULLI (30);
