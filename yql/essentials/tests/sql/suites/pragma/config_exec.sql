/* postgres can not */
USE plato;

INSERT INTO Output
SELECT "1" as key, "1" as subkey, "1" as value;

PRAGMA File("file", "dummy");

INSERT INTO Output
SELECT * from Input where key < "030";
