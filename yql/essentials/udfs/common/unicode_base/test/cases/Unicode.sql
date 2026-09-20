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
    value AS value,
    Unicode::IsUtf(value) AS is,
    Unicode::GetLength(value) AS length,
    Unicode::Substring(value, 1) AS one_end_substring,
    Unicode::Substring(value, 0, 2) AS two_end_substring,
    Unicode::RemoveAll(value, "\xD1\x87пr") AS remove_all,
    Unicode::LevensteinDistance(value, value || Unicode::Substring(value, 0, 5)) AS levenstein,
    Unicode::Reverse(value) AS reverse,
    Unicode::Find(value, "ет"u) AS find,
    Unicode::RFind(value, "ет"u) AS rfind,
    Unicode::Find(value, "ет"u, 7ul) AS find_from,
    Unicode::RFind(value, "ет"u, 7ul) AS rfind_from
FROM AS_TABLE($input)
