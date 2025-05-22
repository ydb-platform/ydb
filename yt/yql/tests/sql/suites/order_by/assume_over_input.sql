/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

INSERT INTO Output
SELECT * FROM Input ASSUME ORDER BY key, subkey, value;
