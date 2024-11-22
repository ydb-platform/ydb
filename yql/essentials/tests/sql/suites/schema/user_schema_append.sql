/* syntax version 1 */
USE plato;

INSERT INTO Input
SELECT * FROM Input WITH SCHEMA Struct<key:String, value:String>;

