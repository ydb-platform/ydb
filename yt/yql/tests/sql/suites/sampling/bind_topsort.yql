/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */

$var = SELECT key, value FROM plato.Input;

SELECT * FROM $var TABLESAMPLE BERNOULLI(10) ORDER BY key ASC, value LIMIT 10;
