USE plato;
PRAGMA DisableSimpleColumns;
PRAGMA yt.JoinMergeTablesLimit = "100";

SELECT
    *
FROM (
    SELECT
        *
    FROM
        Input
    WHERE
        key < "020"
) AS a
LEFT ONLY JOIN (
    SELECT
        subkey
    FROM
        Input
    WHERE
        key < "010"
) AS b
ON
    a.subkey == b.subkey
JOIN /*+ merge() */ (
    SELECT
        key,
        value
    FROM
        Input
) AS c
ON
    a.key == c.key
;
