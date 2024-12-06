/* postgres can not */
/* syntax version 1 */
-- in v1 substring returns Null as a result for missing value
SELECT
    substring(key, 1, 1) AS char,
    substring(value, 1) AS tail
FROM plato.Input;
