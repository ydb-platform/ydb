/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    i1.subkey AS sk,
    WeakField(i1.value1, 'String', 'funny') AS i1v1,
    WeakField(i1.value2, 'String', 'bunny') AS i1v2,
    WeakField(i2.value1, 'String', 'short') AS i2v1,
    WeakField(i2.value2, 'String', 'circuit') AS i2v2
FROM
    Input1 AS i1
JOIN
    Input2 AS i2
ON
    WeakField(i1.value1, 'String') == WeakField(i2.value2, 'String')
WHERE
    WeakField(i2.key, 'String') == '150' OR WeakField(i1.key, 'String') == '075'
ORDER BY
    sk
;
