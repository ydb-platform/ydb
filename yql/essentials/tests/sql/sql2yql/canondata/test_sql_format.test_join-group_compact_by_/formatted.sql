USE plato;

PRAGMA yt.JoinMergeForce = '1';
PRAGMA yt.JoinMergeTablesLimit = '10';

SELECT
    key1,
    subkey1
FROM (
    SELECT
        a.key AS key1,
        a.subkey AS subkey1
    FROM (
        SELECT
            *
        FROM
            Input
        WHERE
            subkey != 'bar'
    ) AS a
    JOIN (
        SELECT
            *
        FROM
            Input
        WHERE
            subkey != 'foo'
    ) AS b
    ON
        a.key == b.key AND a.subkey == b.subkey
)
GROUP COMPACT BY
    key1,
    subkey1
;
