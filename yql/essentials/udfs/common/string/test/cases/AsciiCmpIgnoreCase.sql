SELECT
    value,
    String::AsciiStartsWithIgnoreCase(value, "AS") AS icstarts,
    String::AsciiEndsWithIgnoreCase(value, "AS") AS icends,
FROM Input;
