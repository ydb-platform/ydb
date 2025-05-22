/* syntax version 1 */
USE plato;
SELECT * FROM Input WITH SCHEMA Struct<key:String, subkey:String>;
SELECT * FROM Input WITH SCHEMA Struct<key:String, value:String>;

