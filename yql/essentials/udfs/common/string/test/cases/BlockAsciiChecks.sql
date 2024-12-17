/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    String::IsAscii(value) as isascii,
    String::IsAsciiSpace(value) as isspace,
    String::IsAsciiUpper(value) as isupper,
    String::IsAsciiLower(value) as islower,
    String::IsAsciiDigit(value) as isdigit,
    String::IsAsciiAlpha(value) as isalpha,
    String::IsAsciiAlnum(value) as isalnum,
    String::IsAsciiHex(value) as ishex
FROM Input
