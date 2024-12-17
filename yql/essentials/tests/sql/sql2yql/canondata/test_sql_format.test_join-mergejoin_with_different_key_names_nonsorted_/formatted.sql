PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '100.0';
PRAGMA yt.JoinAllowColumnRenames = 'true';

$unsorted = (
    SELECT
        key,
        CAST((CAST(subkey AS Int32) + 2) AS String) AS sk,
        value AS val
    FROM
        Unsorted
);

FROM
    SortedBySubkeyValue AS a
JOIN
    $unsorted AS b
ON
    a.subkey == b.sk AND a.value == b.val
SELECT
    *
ORDER BY
    a.key,
    b.key
;
