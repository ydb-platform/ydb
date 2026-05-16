$input = AsList(
    <|value:"test"|>,
    <|value:"\xD1\x82\xD0\xB5\xD1\x81\xD1\x82"|>,
    <|value:"TeSt"|>,
    <|value:"\xD1\x82\xD0\x95\xD1\x81\xD0\xA2"|>,
    <|value:"Eyl\xC3\xBCl"|>,
    <|value:"6"|>,
    <|value:""|>
);

SELECT
    value,
    String::AsciiToLower(value) AS ascii_lower,
    String::AsciiToUpper(value) AS ascii_upper,
    String::AsciiToTitle(value) AS ascii_title,
    String::ToLower(value) AS lower,
    String::ToUpper(value) AS upper,
    String::ToTitle(value) AS title,
    String::Reverse(value) AS reverse,
    String::ToByteList(value) AS byte_list,
    String::FromByteList(String::ToByteList(value)) AS from_byte_list,
    String::FromByteList(YQL::LazyList(String::ToByteList(value))) AS from_lazy_byte_list
FROM AS_TABLE($input);
