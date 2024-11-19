/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */

$var = SELECT key, value FROM plato.Input;

SELECT * FROM $var TABLESAMPLE BERNOULLI(10) ORDER BY key ASC, value LIMIT 10;
