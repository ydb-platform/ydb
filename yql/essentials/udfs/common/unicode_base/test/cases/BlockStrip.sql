/* syntax version 1 */

pragma UseBlocks;

SELECT
    value as value,
    Unicode::Strip(value)
From Input
