/* syntax version 1 */
/* postgres can not */
/* hybridfile can not YQL-17764 */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 3 */
USE plato;

PRAGMA DisableSimpleColumns;

$a =
    SELECT
        *
    FROM
        Input
    WHERE
        key > "199" AND value != "bbb"
;

SELECT
    *
FROM (
    SELECT
        a.value
    FROM
        Input AS a
    INNER JOIN
        $a AS b
    USING (subkey)
)
TABLESAMPLE BERNOULLI (40);
