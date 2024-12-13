--!ansi_lexer
/* syntax version 1 */
USE plato;

SELECT
    count(*)
FROM
    Input2 AS a
JOIN
    Input3 AS b
USING ('key');
