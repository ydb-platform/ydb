/* syntax version 1 */
USE plato;
PRAGMA yt.MapJoinLimit = "1m";
PRAGMA yt.DisableOptimizers = "PhysicalOptimizer-FuseInnerMap";

SELECT
    v3
FROM Input1
    AS a
JOIN Input2
    AS b
ON (a.k1 == b.k2)
JOIN Input3
    AS c
ON (a.k1 == c.k3)
ORDER BY
    v3;
