/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 8 */
SELECT * FROM plato.Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1) WHERE subkey > "1";
