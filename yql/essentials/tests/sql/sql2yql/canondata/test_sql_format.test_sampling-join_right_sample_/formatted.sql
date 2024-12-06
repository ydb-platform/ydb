/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
/* syntax version 1 */
USE plato;

PRAGMA DisableSimpleColumns;

SELECT
    *
FROM
    plato.Input AS a
INNER JOIN
    plato.Input AS b
    SAMPLE 0.3
ON
    a.key == b.key
;
