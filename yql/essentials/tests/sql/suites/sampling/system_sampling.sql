/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 100 */
/* syntax version 1 */
USE plato;

EVALUATE FOR $_i IN ListFromRange(0, 10)  DO BEGIN
    INSERT INTO Output
    SELECT * FROM Input;
    COMMIT;
END DO;

SELECT * FROM Output TABLESAMPLE SYSTEM(10);
