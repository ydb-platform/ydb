/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */
pragma direct_read;
SELECT * FROM plato.Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1);
