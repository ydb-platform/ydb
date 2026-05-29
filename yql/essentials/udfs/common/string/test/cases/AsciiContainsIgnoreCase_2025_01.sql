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
    String::AsciiEqualsIgnoreCase(value, "FDSA") AS icequals,
FROM AS_TABLE($input);

