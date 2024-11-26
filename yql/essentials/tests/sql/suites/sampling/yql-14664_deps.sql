/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 10 */
USE plato;

INSERT INTO @a
SELECT
    *
FROM Input
WHERE key > "020";

COMMIT;

SELECT 
    *
FROM (
    SELECT
        *
    FROM  @a
    TABLESAMPLE BERNOULLI(50.0) REPEATABLE(1)
) LIMIT 10
