-- ignore runonopt plan diff
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.JoinMergeTablesLimit = '10';

$i =
    SELECT
        *
    FROM
        concat(Input2, Input3)
    WHERE
        value != '1'
;

SELECT
    b.key,
    b.subkey,
    a.value
FROM
    Input1 AS a
JOIN
    $i AS b
ON
    (a.key, a.subkey) == (b.key, b.subkey)
;
