/* syntax version 1 */
/* postgres can not */

$data = SELECT "a,b,c,d" AS a, "e,f,g,h" AS b, "x" AS c;

SELECT bb,count(*) as count FROM $data FLATTEN BY (String::SplitToList(a, ",") as a, String::SplitToList(b, ",") as bb) GROUP BY bb ORDER BY bb,count;

