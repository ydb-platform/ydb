PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
/* ignore yt detailed plan diff */
USE plato;
PRAGMA yt.MapJoinLimit = "10M";

$sizes = (
    SELECT
        0 AS id
    FROM Input
);

SELECT
    d.key
FROM Input
    AS d
CROSS JOIN $sizes
    AS s
ORDER BY
    d.key;
