USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3";
PRAGMA yt.JoinAllowColumnRenames = "true";
PRAGMA SimpleColumns;

FROM
    Input3 AS c
JOIN
    Input4 AS d
ON
    c.k3 == d.k4
RIGHT ONLY JOIN
    Input1 AS a
ON
    a.k1 == c.k3 AND a.v1 == c.v3
SELECT
    *
ORDER BY
    u1
;
