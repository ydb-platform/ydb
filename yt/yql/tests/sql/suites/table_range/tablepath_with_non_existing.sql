/* syntax version 1 */
/* kikimr can not - range not supported */
USE plato;
SELECT key, subkey, TableName() AS name
FROM each(["Input1", "Input2", "Input3"])
WITH SCHEMA Struct<key:String, subkey:String, value:String>;
