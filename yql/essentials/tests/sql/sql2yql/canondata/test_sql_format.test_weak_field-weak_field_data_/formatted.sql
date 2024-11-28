/* postgres can not */
USE plato;

SELECT
    subkey,
    WeakField(data1, "Int32", 32) AS d1,
    WeakField(data3, "Int32", 32) AS d3
FROM Input3
ORDER BY
    subkey;
