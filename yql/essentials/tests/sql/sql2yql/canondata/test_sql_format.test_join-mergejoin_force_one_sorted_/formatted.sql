USE plato;

PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeForce = "true";
PRAGMA yt.JoinMergeUnsortedFactor = "0";

SELECT
    a.key AS key
FROM
    InputSorted AS a
JOIN
    Input AS b
USING (key)
ORDER BY
    key
;
