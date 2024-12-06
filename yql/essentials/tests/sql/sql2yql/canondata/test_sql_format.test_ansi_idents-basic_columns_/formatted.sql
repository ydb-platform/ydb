--!ansi_lexer
/* syntax version 1 */
USE plato;

SELECT
    "key" || subkey AS "akey"
FROM Input
ORDER BY
    akey;
