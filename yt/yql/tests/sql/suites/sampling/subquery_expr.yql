/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */

$count = SELECT COUNT(*) FROM plato.Input; -- $count = 10

SELECT * FROM (SELECT * FROM plato.Input) TABLESAMPLE BERNOULLI(5 * $count);
