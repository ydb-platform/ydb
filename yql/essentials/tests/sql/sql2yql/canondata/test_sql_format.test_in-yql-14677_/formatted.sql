/* postgres can not */
USE plato;

PRAGMA yt.MapJoinLimit = '1m';

$l1 = (
    SELECT
        key
    FROM
        `Input`
);

SELECT
    *
FROM
    Input
WHERE
    TRUE
    AND value != ''
    AND key IN $l1
;
