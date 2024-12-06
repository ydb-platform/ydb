/* custom error:Table "Input" row type differs from the written row type: Struct<-subkey:String>*/
USE plato;

INSERT INTO Input
SELECT * FROM Input WITH SCHEMA Struct<key:String, value:String>;

