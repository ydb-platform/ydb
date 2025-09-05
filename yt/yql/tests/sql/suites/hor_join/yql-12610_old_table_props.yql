/* postgres can not */
/* syntax version 1 */
/* kikimr can not - yt pragma */
USE plato;
pragma yt.UseSystemColumns="0";

SELECT * FROM (
    select key, TableRecordIndex() as record, TablePath() as path from Input
    union all
    select key, TableRecordIndex() as record, "d" as path from Input
) AS x
ORDER BY key, record, path
;
