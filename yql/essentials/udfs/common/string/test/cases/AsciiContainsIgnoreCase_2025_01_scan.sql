SELECT
    value,
    String::AsciiContainsIgnoreCase(value, "AS") AS iccontains,
    String::AsciiContainsIgnoreCase(value, "") AS icempty,
    String::AsciiEqualsIgnoreCase(value, "FDSA") AS icequals,
FROM Input;

