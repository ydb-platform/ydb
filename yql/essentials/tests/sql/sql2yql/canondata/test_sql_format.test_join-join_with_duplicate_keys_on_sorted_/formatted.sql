PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '3.0';

FROM
    SortedByKeySubkey AS a
JOIN
    SortedByKey AS b
ON
    a.key == b.key AND a.subkey == b.key
SELECT
    *
ORDER BY
    a.value,
    b.value
;
