/* syntax version 1 */
/* postgres can not */
USE plato;

pragma yt.DisableOptimizers="HorizontalJoin,MultiHorizontalJoin";

SELECT * FROM (
    select key, TableRecordIndex() as record, TablePath() as path from Input
    union all
    select key, TableRecordIndex() as record, "d" as path from Input
) AS x
ORDER BY key, record, path
;
