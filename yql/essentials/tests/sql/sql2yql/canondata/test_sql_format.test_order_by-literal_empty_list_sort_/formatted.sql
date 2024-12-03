/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;
$list = ListCreate(Struct<key: String, subkey: String, value: String>);

INSERT INTO Output
SELECT
    *
FROM as_table($list)
ORDER BY
    key;
