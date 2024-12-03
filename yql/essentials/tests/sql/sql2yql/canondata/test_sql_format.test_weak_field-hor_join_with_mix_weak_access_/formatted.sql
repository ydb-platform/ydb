/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    key,
    WeakField(value1, "String", "funny") AS value
FROM Input
UNION ALL
SELECT
    key,
    _other["value1"] AS value
FROM Input;
