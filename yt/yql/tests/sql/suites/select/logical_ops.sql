/* postgres can not */
SELECT
    LENGTH(key) == 3 AND LENGTH(value) == 3 as and_res,
    LENGTH(key) == 3 OR  LENGTH(value) == 3 as or_res,
    LENGTH(key) == 3 XOR LENGTH(value) == 3 as xor_res
FROM plato.Input;