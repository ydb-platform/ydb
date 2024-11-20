/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

INSERT INTO @tmp
SELECT * FROM Input TABLESAMPLE BERNOULLI(30) REPEATABLE(1);

COMMIT;

SELECT * FROM @tmp;
