/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */

$count = SELECT COUNT(*) FROM plato.Input; -- $count = 10

SELECT * FROM (SELECT * FROM plato.Input) TABLESAMPLE BERNOULLI(5 * $count);
