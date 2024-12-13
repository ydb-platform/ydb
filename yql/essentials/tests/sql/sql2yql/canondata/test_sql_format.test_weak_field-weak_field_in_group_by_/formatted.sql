/* postgres can not */
USE plato;

SELECT
    sk,
    count(*) AS count
FROM
    Input
GROUP BY
    WeakField(uiData, 'uint64') AS sk
ORDER BY
    sk
;
