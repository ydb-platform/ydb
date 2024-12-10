/* postgres can not */
USE plato;

PRAGMA yt.MapJoinLimit = "1M";

INSERT INTO @a
SELECT
    *
FROM
    Input
;

COMMIT;

SELECT
    count(*)
FROM
    @a
;

SELECT
    *
FROM
    @a
LIMIT 2;

SELECT
    count(*)
FROM
    @a AS a
CROSS JOIN
    @a AS b
;

COMMIT;
