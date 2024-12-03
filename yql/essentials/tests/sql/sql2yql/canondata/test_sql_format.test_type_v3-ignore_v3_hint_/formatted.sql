/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    Yson::LookupString(subkey, "a") AS a,
FROM Input
    WITH ignore_type_v3;
