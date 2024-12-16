/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    odd,
    sum(WeakField(data3, "int32") + WeakField(datahole3, "uint32", 999)) AS score
FROM
    Input4
GROUP BY
    CAST(subkey AS uint32) % 2 AS odd
ORDER BY
    odd,
    score
;
