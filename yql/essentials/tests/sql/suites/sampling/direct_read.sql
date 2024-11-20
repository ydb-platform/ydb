/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
pragma direct_read;
SELECT * FROM plato.Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1);
