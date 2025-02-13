/* syntax version 1 */

pragma UseBlocks;

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value)
FROM Input 
WHERE key = "with_format_1" 
   OR key = "with_format_2"
   OR key = "with_format_3"
   OR key = "binary_1"
   OR key = "binary_2";

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value, 2),
    Unicode::ToUint64(value, 16)
FROM Input
WHERE key = "binary_1" 
   OR key = "binary_2";

SELECT
    value AS value,
    key AS key,
    Unicode::ToUint64(value, 8),
    Unicode::ToUint64(value, 10),
    Unicode::ToUint64(value, 16)
FROM Input
WHERE key = "zero";
