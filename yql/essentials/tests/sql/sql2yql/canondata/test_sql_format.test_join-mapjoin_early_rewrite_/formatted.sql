/* postgres can not */
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.MapJoinLimit = "1m";

$subq = (
    SELECT
        CAST((CAST(subkey AS Int32) + 1) AS String) AS subkey_plus_one
    FROM
        Input
);

SELECT
    *
FROM
    Input AS a
JOIN
    $subq AS b
ON
    a.subkey == b.subkey_plus_one
ORDER BY
    subkey,
    key
;
