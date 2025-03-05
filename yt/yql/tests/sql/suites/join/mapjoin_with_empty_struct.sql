PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
/* ignore yt detailed plan diff */
use plato;
pragma yt.MapJoinLimit="10M";

$sizes = (
    SELECT
        0 AS id
    FROM Input
);

SELECT d.key FROM Input as d CROSS JOIN $sizes as s ORDER BY d.key;
