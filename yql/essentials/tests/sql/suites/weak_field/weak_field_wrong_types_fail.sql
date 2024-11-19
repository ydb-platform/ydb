/* postgres can not */
use plato;
SELECT
  WeakField(first_num, "int32", 42) -- first_num column have another type
FROM Input
