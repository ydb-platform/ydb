/* syntax version 1 */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3";
PRAGMA yt.JoinAllowColumnRenames = "true";

$semi =
    SELECT
        *
    FROM Input3
        AS c
    JOIN Input4
        AS d
    ON c.k3 == d.k4;

FROM $semi
    AS semi
RIGHT SEMI JOIN Input1
    AS a
ON a.k1 == semi.k3 AND a.v1 == semi.v3
JOIN Input2
    AS b
ON b.k2 == a.k1 AND b.v2 == a.v1
SELECT
    *
ORDER BY
    u1;
