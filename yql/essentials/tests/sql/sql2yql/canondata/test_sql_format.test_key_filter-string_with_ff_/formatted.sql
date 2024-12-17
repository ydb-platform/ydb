/* syntax version 1 */
/* postgres can not */
USE plato;

INSERT INTO @src
SELECT
    '\xff\xff' || key AS key
FROM
    Input
ORDER BY
    key
;

COMMIT;

SELECT
    count(*)
FROM (
    SELECT
        *
    FROM
        @src
    WHERE
        StartsWith(key, '\xff\xff') AND EndsWith(key, '5')
);
