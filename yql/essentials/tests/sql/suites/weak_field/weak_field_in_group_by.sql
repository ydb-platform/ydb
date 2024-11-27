/* postgres can not */
USE plato;

SELECT
    sk,
    count(*) as count
FROM Input
GROUP BY WeakField(uiData, 'uint64') as sk
ORDER BY sk