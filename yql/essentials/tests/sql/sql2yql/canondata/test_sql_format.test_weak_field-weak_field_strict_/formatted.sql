/* postgres can not */
USE plato;

SELECT
    WeakField(first_num, "uint32") AS num_nodef,
    WeakField(first_num, "uint32", 11) AS num_def,
    WeakField(first_null, "uint32") AS null_nodef,
    WeakField(first_null, "uint32", 42) AS null_def,
    WeakField(val, "string") AS missed_nodef,
    WeakField(val, "string", "no value") AS missed_def
FROM Input;
