/* postgres can not */
/* syntax version 1 */
PRAGMA yt.InferSchema="2";

USE plato;

SELECT
    WeakField(key, "String", "funny"),
    WeakField(subkey, "String", "bunny"),
    WeakField(value, "String", "boom")
FROM RANGE(``, ``, ``)
--FROM Concat(Input1, Input2)
