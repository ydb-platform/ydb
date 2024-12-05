/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
/* ignore plan diff */
USE plato;
PRAGMA DisableSimpleColumns;

SELECT
    *
FROM plato.Input
    AS a
    SAMPLE 0.3
INNER JOIN plato.Input
    AS b
ON a.key == b.key;
