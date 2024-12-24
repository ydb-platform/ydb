PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinAllowColumnRenames = 'true';
PRAGMA yt.JoinMergeUseSmallAsPrimary = 'true';

-- Input2 is smaller than Input1
SELECT
    *
FROM
    Input2 AS b
JOIN
    Input1 AS a
ON
    a.k1 == b.k2
ORDER BY
    a.v1,
    b.v2
;
