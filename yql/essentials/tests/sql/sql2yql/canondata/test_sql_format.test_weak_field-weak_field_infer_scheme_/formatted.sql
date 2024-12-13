/* postgres can not */
/* kikimr can not */
USE plato;

PRAGMA yt.InferSchema;

SELECT
    key,
    subkey,
    WeakField(value, "String") AS value
FROM
    Input
;
