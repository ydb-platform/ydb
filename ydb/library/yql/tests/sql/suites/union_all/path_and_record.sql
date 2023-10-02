/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT * FROM (
    select key, value, TablePath() as path, TableRecordIndex() as record from Input
    union all
    select key, value, "" as path, TableRecordIndex() as record from Input
)
ORDER BY key, path, record
;
