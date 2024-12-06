USE plato;
/* postgres can not */
/* kikimr can not */
PRAGMA DisableSimpleColumns;
/* yt_local_var: MAP_JOIN_LIMIT = 30 */
/* yqlrun_var: MAP_JOIN_LIMIT = 1000 */
PRAGMA yt.MapJoinLimit = "MAP_JOIN_LIMIT";
PRAGMA yt.MapJoinShardCount = "10";

SELECT
    *
FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    a.key == b.key AND a.subkey == b.key
;
