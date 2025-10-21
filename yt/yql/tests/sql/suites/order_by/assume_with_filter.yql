/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

INSERT INTO Output
SELECT
    "3" || key as key,
    subkey,
    value
FROM Input
WHERE key >= "0"
ASSUME ORDER BY key, subkey;
