USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';

$i = (
    SELECT
        a.key AS key,
        a.subkey AS subkey,
        b.value AS value
    FROM
        Input AS a
    INNER JOIN
        Input AS b
    USING (key, subkey)
    ASSUME ORDER BY
        key,
        subkey
);

INSERT INTO Output
SELECT
    AsTuple(key, subkey) AS k,
    value || 'a' AS v
FROM
    $i
ASSUME ORDER BY
    k
;
