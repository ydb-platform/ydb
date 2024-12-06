/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
/* ignore plan diff */
/* syntax version 1 */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "2";
PRAGMA DisableSimpleColumns;

SELECT
    *
FROM
    plato.Input AS a
    SAMPLE 0.3
INNER JOIN
    plato.Input AS b
ON
    a.key == b.key
;
