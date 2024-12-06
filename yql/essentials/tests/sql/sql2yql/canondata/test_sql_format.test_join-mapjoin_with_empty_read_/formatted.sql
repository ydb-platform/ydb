PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
USE plato;
PRAGMA yt.mapjoinlimit = "1m";

$cnt = (
    SELECT
        count(*)
    FROM Input
);
$offset = ($cnt + 10) ?? 0;

$in1 = (
    SELECT
        key
    FROM Input
    WHERE key != ""
    ORDER BY
        key
    LIMIT 10 OFFSET $offset
);

SELECT
    *
FROM Input
    AS a
LEFT JOIN $in1
    AS b
ON a.key == b.key;
$limit = ($cnt / 100) ?? 0;

$in2 = (
    SELECT
        key
    FROM Input
    WHERE key != ""
    LIMIT $limit
);

SELECT
    *
FROM Input
    AS a
LEFT ONLY JOIN $in2
    AS b
ON a.key == b.key;
