/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    value,
    String::AsciiContainsIgnoreCase(value, "AS") AS iccontains,
    String::AsciiContainsIgnoreCase(value, "") AS icempty,
    String::AsciiStartsWithIgnoreCase(value, "AS") AS icstarts,
    String::AsciiEndsWithIgnoreCase(value, "AS") AS icends,
    String::AsciiEqualsIgnoreCase(value, "FDSA") AS icequals,
FROM Input;
