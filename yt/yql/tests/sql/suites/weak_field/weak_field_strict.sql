/* postgres can not */
use plato;
SELECT
  WeakField(first_num, "uint32") as num_nodef,
  WeakField(first_num, "uint32", 11) as num_def,
  WeakField(first_null, "uint32") as null_nodef,
  WeakField(first_null, "uint32", 42) as null_def,
  WeakField(val, "string") as missed_nodef,
  WeakField(val, "string", "no value") as missed_def
FROM Input
