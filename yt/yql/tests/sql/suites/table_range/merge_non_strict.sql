/* postgres can not */
USE plato;

SELECT key, subkey FROM CONCAT(Input1, Input1) ORDER BY key, subkey;

SELECT key, subkey FROM CONCAT(Input1, Input2) ORDER BY key, subkey;
