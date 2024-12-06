/* postgres can not */
SELECT
    LENGTH(key) == 3 AND LENGTH(value) == 3 AS and_res,
    LENGTH(key) == 3 OR LENGTH(value) == 3 AS or_res,
    LENGTH(key) == 3 XOR LENGTH(value) == 3 AS xor_res
FROM plato.Input;
