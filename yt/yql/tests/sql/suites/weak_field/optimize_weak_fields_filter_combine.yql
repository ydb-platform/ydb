/* kikimr can not */
pragma yt.InferSchema;
USE plato;
SELECT min(key),subkey,max(WeakField(value, "String")) FROM Input where WeakField(subkey, "Int64") > 0 group by WeakField(subkey, "Int64") as subkey order by subkey;
