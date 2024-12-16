/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.MapJoinLimit = "1m";

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
