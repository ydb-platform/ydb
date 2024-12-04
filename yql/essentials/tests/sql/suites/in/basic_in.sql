/* postgres can not */
SELECT
    1 IN (1, 2),
    3 NOT IN (1, 2),
    "1" IN (key, subkey, value),
    key NOT IN (key, subkey, value),
    key NOT IN AsList(subkey),
    CAST(subkey AS Int32) IN (1, 2) AS optional_key_i32,
    CAST(subkey AS Int64) IN (1, 2) AS optional_key_i64,
    CAST(subkey AS Uint32) IN (1, 2) AS optional_key_ui32,
    CAST(subkey AS Uint64) IN (1, 2) AS optional_key_ui64,
    CAST(subkey AS Uint8) IN (1, 2) AS optional_key_to_larger_type
FROM plato.Input;
