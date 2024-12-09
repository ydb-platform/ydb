/* syntax version 1 */
/* postgres can not */
PRAGMA yt.MapJoinLimit = "1m";
USE plato;
$t = [<|"x": "150", "y": 1, "z": NULL|>, <|"x": "150", "y": 2, "z": NULL|>];

SELECT
    *
FROM Input1
    AS a
LEFT JOIN AS_TABLE($t)
    AS b
ON a.key == b.x
ORDER BY
    key,
    y;
