/* postgres can not */
/* kikimr can not */
USE plato;

PRAGMA DisableSimpleColumns;
PRAGMA yt.MapJoinLimit = '1m';

INSERT INTO @tmp
SELECT
    *
FROM
    Input
WHERE
    key > '100'
;

COMMIT;

SELECT
    *
FROM
    Input AS a
LEFT JOIN
    @tmp AS b
ON
    a.key == b.key
;
