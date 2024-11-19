/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 20 */
USE plato;

$var = SELECT * FROM Input;

INSERT INTO @tmp
SELECT * FROM $var TABLESAMPLE BERNOULLI(100);

INSERT INTO @tmp
SELECT * FROM $var TABLESAMPLE BERNOULLI(50);

COMMIT;

SELECT * FROM @tmp;
