/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 4 */
/* ignore plan diff */
USE plato;

SELECT a.*, row_number() OVER (PARTITION BY key, subkey) as rn, TableName() as tn
FROM Input as a
TABLESAMPLE BERNOULLI(10.0) REPEATABLE(1)
;
