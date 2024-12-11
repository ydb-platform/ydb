USE plato;

/* postgres can not */
/* kikimr can not */
PRAGMA DisableSimpleColumns;
PRAGMA yt.MapJoinLimit = "1m";

SELECT
    *
FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    a.key == b.key AND a.subkey == b.key
;
