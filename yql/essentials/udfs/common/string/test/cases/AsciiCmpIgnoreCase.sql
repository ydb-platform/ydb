$input = AsList(
    <|value:"fdsa"|>,
    <|value:"aswedfg"|>,
    <|value:"asdadsaasd"|>,
    <|value:"gdsfsassas"|>,
    <|value:""|>,
    <|value:"`Привет, мир!`"|>
);

SELECT
    value,
    String::AsciiContainsIgnoreCase(value, "AS") AS iccontains,
    String::AsciiContainsIgnoreCase(value, "") AS icempty,
    String::AsciiStartsWithIgnoreCase(value, "AS") AS icstarts,
    String::AsciiEndsWithIgnoreCase(value, "AS") AS icends,
    String::AsciiEqualsIgnoreCase(value, "FDSA") AS icequals,
FROM AS_TABLE($input);
