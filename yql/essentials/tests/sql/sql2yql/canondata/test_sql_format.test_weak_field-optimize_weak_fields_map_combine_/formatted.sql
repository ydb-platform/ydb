/* kikimr can not */
PRAGMA yt.InferSchema;

USE plato;

SELECT
    min(key),
    subkey,
    max(WeakField(value, 'String'))
FROM
    Input
GROUP BY
    WeakField(subkey, 'Int64') AS subkey
ORDER BY
    subkey
;
