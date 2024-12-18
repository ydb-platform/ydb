PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.LookupJoinLimit = '64k';
PRAGMA yt.LookupJoinMaxRows = '100';

SELECT
    *
FROM
    Input1 AS a
INNER JOIN (
    SELECT
        *
    FROM
        Input2
    WHERE
        k2 == 'not_existent'
) AS b
ON
    a.k1 == b.k2
;
