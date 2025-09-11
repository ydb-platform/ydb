/* postgres can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) < 100 */
/* syntax version 1 */
USE plato;

EVALUATE FOR $_i IN ListFromRange(0, 10)  DO BEGIN
    INSERT INTO Output
    SELECT * FROM Input;
    COMMIT;
END DO;

SELECT * FROM Output TABLESAMPLE SYSTEM(10);
