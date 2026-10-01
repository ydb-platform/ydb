$input = [
    <|value: "Eyl\xC3\xBCl"u|>,
    <|value: "\xD0\xB6\xD0\xBD\xD1\x96\xD1\x9E\xD0\xBD\xD1\x8F"u|>,
    <|value: "\xC3\xBAnora"u|>,
    <|value: "Ci\xD1\x87 Ci\xD1\x87"u|>,
    <|value: "\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82 \xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82"u|>,
    <|value: "6"u|>,
    <|value: ""u|>,
];

SELECT
    Unicode::ToCodePointList(value) AS code_point_list,
    Unicode::FromCodePointList(Unicode::ToCodePointList(value)) AS from_code_point_list,
    Unicode::FromCodePointList(YQL::LazyList(Unicode::ToCodePointList(value))) AS from_lazy_code_point_list,
FROM AS_TABLE($input)
