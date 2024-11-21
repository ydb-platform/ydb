/* postgres can not */
/* kikimr can not */
USE plato;
pragma yt.InferSchema;

SELECT key, subkey, WeakField(value, "String") as value FROM Input;
