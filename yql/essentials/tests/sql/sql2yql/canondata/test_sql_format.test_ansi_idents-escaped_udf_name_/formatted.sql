--!ansi_lexer
/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    `key`
FROM
    Input
WHERE
    'String'::Contains('key', '7')
ORDER BY
    key
;
