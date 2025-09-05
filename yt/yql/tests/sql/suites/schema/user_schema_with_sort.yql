/* syntax version 1 */
USE plato;

SELECT * FROM Input1 WITH SCHEMA Struct<key:String, value:String> ORDER BY key;
SELECT * FROM Input1 WITH SCHEMA Struct<key:String?, subkey:String> ORDER BY key; -- should reset sort
SELECT * FROM Input2 WITH SCHEMA Struct<key:String, subkey:String> ORDER BY key DESC;
