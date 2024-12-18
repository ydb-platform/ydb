/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    subkey,
    WeakField(data3, 'int32') AS data3,
    WeakField(datahole3, 'uint32', 999) AS holes3
FROM
    Input4
ORDER BY
    subkey
;
