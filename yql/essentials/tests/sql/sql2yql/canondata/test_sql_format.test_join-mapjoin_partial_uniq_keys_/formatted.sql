PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
/* ignore runonopt plan diff */
USE plato;
PRAGMA yt.MapJoinLimit = "1m";

-- YQL-5582
$join = (
    SELECT
        a.key AS key,
        a.subkey AS subkey,
        a.value AS value
    FROM (
        SELECT
            *
        FROM
            Input
        WHERE
            value > "bbb"
    ) AS a
    LEFT JOIN
        Input AS b
    ON
        a.key == b.key
);

SELECT
    count(*)
FROM
    $join
;
