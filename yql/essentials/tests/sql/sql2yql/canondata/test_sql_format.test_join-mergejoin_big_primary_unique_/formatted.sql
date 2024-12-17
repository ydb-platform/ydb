PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinAllowColumnRenames = 'true';
PRAGMA yt.JoinMergeUseSmallAsPrimary = 'false';

-- Input2 is smaller than Input1, but Input1 has unique keys
SELECT
    *
FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    a.k1 == b.k2 AND a.v1 == b.v2
;
