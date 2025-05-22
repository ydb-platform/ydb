/* postgres can not */
SELECT
    LENGTH(key) & LENGTH(value) as and_res,
    LENGTH(key) | LENGTH(value) as or_res,
    LENGTH(key) ^ LENGTH(value) as xor_res,
    LENGTH(key) << 1 as shl_res,
    LENGTH(key) >> 1 as shr_res,
    LENGTH(key) |<< 15 as rotl_res,
    LENGTH(key) >>| 15 as rotr_res,
    ~LENGTH(key) as not_res
FROM plato.Input;