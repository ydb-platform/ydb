PRAGMA DisableSimpleColumns;
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";
PRAGMA yt.JoinAllowColumnRenames = "false";

FROM
    SortedBySubkeyValue AS a
JOIN
    SortedByKey AS b
ON
    a.subkey == b.key
SELECT
    *
ORDER BY
    a.subkey,
    b.key
;

FROM
    SortedBySubkeyValue AS a
JOIN
    SortedByKey AS b
ON
    a.subkey == b.key
SELECT
    a.subkey,
    b.key
ORDER BY
    a.subkey,
    b.key
;

FROM
    SortedBySubkeyValue AS a
LEFT JOIN
    SortedByKey AS b
ON
    a.subkey == b.key
SELECT
    *
ORDER BY
    a.subkey
;

FROM
    SortedBySubkeyValue AS a
RIGHT JOIN
    SortedByKey AS b
ON
    a.subkey == b.key
SELECT
    *
ORDER BY
    b.key
;
