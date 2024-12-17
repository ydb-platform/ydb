PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.LookupJoinLimit = '64k';
PRAGMA yt.LookupJoinMaxRows = '100';

-- no lookup join in this case
SELECT
    *
FROM
    Input1 AS a
LEFT JOIN
    Input2 AS b
ON
    a.k1 == b.k2
;
