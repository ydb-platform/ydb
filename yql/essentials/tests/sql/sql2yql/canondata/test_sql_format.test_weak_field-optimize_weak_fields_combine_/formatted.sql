/* kikimr can not */
PRAGMA yt.InferSchema;

USE plato;

SELECT
    key,
    min(WeakField(subkey, 'Int64')),
    max(WeakField(value, 'String'))
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
