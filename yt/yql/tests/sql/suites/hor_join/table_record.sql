/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT * FROM (
    select key, TableRecordIndex() as record, TablePath() as path from Input1
    union all
    select key, TableRecordIndex() as record, "d" as path from Input2
) AS x
ORDER BY key, record, path
;
