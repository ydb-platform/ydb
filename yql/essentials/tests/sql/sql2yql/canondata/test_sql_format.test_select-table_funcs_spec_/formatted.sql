/* syntax version 1 */
/* postgres can not */
/* kikimr can not */
USE plato;

--insert into Output
SELECT
    TablePath() AS table_path,
    TableRecordIndex() AS table_rec,
    TableName("foo/bar") AS table_name1,
    TableName("baz") AS table_name2,
    TableName() AS table_name3
FROM
    Input
WHERE
    key == '800'
;
