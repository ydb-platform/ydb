/* syntax version 1 */
USE plato;

INSERT INTO Input WITH TRUNCATE
SELECT * FROM Input WITH SCHEMA Struct<key:String, value:String>;
