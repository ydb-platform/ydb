/* postgres can not */
SELECT
    LENGTH(key) & LENGTH(value) AS and_res,
    LENGTH(key) | LENGTH(value) AS or_res,
    LENGTH(key) ^ LENGTH(value) AS xor_res,
    LENGTH(key) << 1 AS shl_res,
    LENGTH(key) >> 1 AS shr_res,
    LENGTH(key) |<< 15 AS rotl_res,
    LENGTH(key) >>| 15 AS rotr_res,
    ~LENGTH(key) AS not_res
FROM plato.Input;
