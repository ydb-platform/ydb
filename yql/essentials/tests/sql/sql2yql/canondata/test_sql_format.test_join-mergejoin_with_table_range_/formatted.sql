PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";

SELECT
    *
FROM
    CONCAT("Left1", "Left2") AS a
INNER JOIN
    CONCAT("Right1", "Right2") AS b
USING (key)
ORDER BY
    a.value,
    b.value
;
