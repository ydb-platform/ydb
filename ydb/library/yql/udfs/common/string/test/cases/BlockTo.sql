/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    value,
    String::AsciiToLower(value) AS ascii_lower,
    String::AsciiToUpper(value) AS ascii_upper,
    String::AsciiToTitle(value) AS ascii_title,
FROM Input;
