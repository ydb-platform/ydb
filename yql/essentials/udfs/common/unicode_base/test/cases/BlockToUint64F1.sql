/* syntax version 1 */

pragma UseBlocks;

SELECT
    value as value,
    Unicode::ToUint64(value),
FROM Input

