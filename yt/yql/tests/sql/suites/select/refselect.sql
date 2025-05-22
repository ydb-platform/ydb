/* postgres can not */
USE plato;
PRAGMA refselect;
SELECT key, subkey, value FROM `test_table_src`;
