/* kikimr can not */
PRAGMA yt.InferSchema;

USE plato;

SELECT
    key,
    WeakField(subkey, "Int64"),
    WeakField(value, "String")
FROM
    Input
;
