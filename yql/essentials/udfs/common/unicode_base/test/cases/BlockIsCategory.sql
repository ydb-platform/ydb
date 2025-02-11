/* syntax version 1 */

pragma UseBlocks;

SELECT
    value as value,
    Unicode::IsAscii(value),
    Unicode::IsSpace(value),
    Unicode::IsUpper(value),
    Unicode::IsLower(value),
    Unicode::IsDigit(value),
    Unicode::IsAlpha(value),
    Unicode::IsAlnum(value),
    Unicode::IsHex(value),
    Unicode::IsUnicodeSet(value, "[вао]"u)
FROM Input
