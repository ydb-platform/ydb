USE plato;

PRAGMA yt.MapJoinLimit = '1M';

$t = (
    SELECT
        l.key AS key,
        r.subkey AS subkey,
        l.value || r.value AS value
    FROM
        Input1 AS l
    CROSS JOIN
        Input2 AS r
);

SELECT
    l.*,
    r.value AS rvalue
FROM
    $t AS l
LEFT JOIN
    Input3 AS r
ON
    l.key == coalesce('' || r.key, '')
;
