/* postgres can not */
USE plato;

SELECT
    subkey,
    WeakField(data3, "int32") AS data3str,
    WeakField(datahole3, "int32", 999) AS holes3
FROM Input3
ORDER BY
    subkey;
