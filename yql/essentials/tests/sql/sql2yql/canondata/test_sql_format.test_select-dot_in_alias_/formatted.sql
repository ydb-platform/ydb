/* postgres can not */
USE plato;
PRAGMA AllowDotInAlias;

--INSERT INTO Output
SELECT
    key AS `.key`,
    subkey AS `sub.key`,
    value AS `value.`
FROM
    Input
ORDER BY
    `.key`,
    `sub.key`
;
