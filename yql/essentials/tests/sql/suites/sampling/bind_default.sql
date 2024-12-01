/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

$var = SELECT * FROM Input;

SELECT * FROM $var TABLESAMPLE BERNOULLI(50);
