/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
SELECT * FROM plato.Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1) LIMIT 10;