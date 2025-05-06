/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    value,
    String::AsciiStartsWithIgnoreCase(value, "AS") AS icstarts,
    String::AsciiEndsWithIgnoreCase(value, "AS") AS icends,
FROM Input;
