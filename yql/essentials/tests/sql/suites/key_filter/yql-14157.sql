/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT v1, v2
FROM RANGE("", "Input1", "Input2")
WHERE key == "037";

SELECT v1, v2
FROM RANGE("", "Input1", "Input2")
WHERE key == "150" and subkey = "1";

