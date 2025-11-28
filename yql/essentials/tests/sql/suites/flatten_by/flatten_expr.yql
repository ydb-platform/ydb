/* syntax version 1 */
/* postgres can not */

$data = SELECT "a,b,c,d" AS a, "e,f,g,h" AS b, "x" AS c;

SELECT a,bb,c FROM $data FLATTEN BY (String::SplitToList(a, ",") as a, String::SplitToList(b, ",") as bb) ORDER BY a,bb;

