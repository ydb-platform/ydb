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
    String::HasPrefixIgnoreCase(value, "AS") AS icprefix,
    String::StartsWithIgnoreCase(value, "AS") AS icstarts,
    String::HasSuffixIgnoreCase(value, "AS") AS icsuffix,
    String::EndsWithIgnoreCase(value, "AS") AS icends,
FROM AS_TABLE($input);
