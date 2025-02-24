/* syntax version 1 */
SELECT
    value as value,
    Unicode::ToUint64(value, 1),
FROM Input

