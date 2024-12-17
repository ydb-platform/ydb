USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeForce = 'true';

SELECT
    a.key AS key
FROM
    Input1 AS a
JOIN
    Input2 AS b
USING (key)
ORDER BY
    key
;
