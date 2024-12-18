/* syntax version 1 */
USE plato;

SELECT
    *
FROM
    range('', 'foo', 'foo') WITH SCHEMA Struct<Key: String>
;

SELECT
    *
FROM
    Range_strict('', 'foo', 'foo') WITH SCHEMA Struct<Key: String>
;

SELECT
    *
FROM
    Each(ListCreate(String)) WITH SCHEMA Struct<Key: Int32>
;

SELECT
    *
FROM
    Each_strict(ListCreate(String)) WITH SCHEMA Struct<Key: Int32>
;
