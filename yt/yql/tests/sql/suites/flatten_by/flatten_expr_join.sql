/* syntax version 1 */
/* postgres can not */
USE plato;

$data = SELECT "075,020,075,020" AS a, "x" AS c;

SELECT * FROM ANY $data as x FLATTEN BY (String::SplitToList(a, ",") as aa) JOIN Input as y ON x.aa = y.key ORDER BY aa;

