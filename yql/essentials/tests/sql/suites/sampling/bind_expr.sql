/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

$count = SELECT COUNT(*) FROM Input; -- $count = 10

$var = SELECT * FROM Input;

SELECT * FROM $var TABLESAMPLE BERNOULLI(5 * $count);
