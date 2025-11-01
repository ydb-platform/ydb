/* syntax version 1 */
/* postgres can not */

USE plato;

$t = [<|"x":"150", "y":1, "z":Null|>, <|"x":"150", "y":2, "z":Null|>];

SELECT * FROM Input1 AS a LEFT JOIN AS_TABLE($t) AS b ON a.key = b.x ORDER BY key, y;

