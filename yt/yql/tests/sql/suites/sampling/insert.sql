/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 10 */
USE plato;

INSERT INTO @tmp
SELECT * FROM Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1);

COMMIT;

SELECT * FROM @tmp;
