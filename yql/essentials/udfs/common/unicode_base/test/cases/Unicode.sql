/* syntax version 1 */
SELECT
    value AS value,
    Unicode::Normalize(value) AS normalize,
    Unicode::IsUtf(value) AS is,
    Unicode::GetLength(value) AS length,
    Unicode::Substring(value, 1) AS one_end_substring,
    Unicode::Substring(value, 0, 2) AS two_end_substring,
    Unicode::RemoveAll(value, "\xD1\x87пr") AS remove_all,
    Unicode::LevensteinDistance(value, value || Unicode::Substring(value, 0, 5)) AS levenstein,
    Unicode::ToCodePointList(value) AS code_point_list,
    Unicode::FromCodePointList(Unicode::ToCodePointList(value)) AS from_code_point_list,
    Unicode::FromCodePointList(YQL::LazyList(Unicode::ToCodePointList(value))) AS from_lazy_code_point_list,
    Unicode::Reverse(value) AS reverse,
    Unicode::Find(value, "ет"u) AS find,
    Unicode::RFind(value, "ет"u) AS rfind,
    Unicode::Find(value, "ет"u, 7ul) AS find_from,
    Unicode::RFind(value, "ет"u, 7ul) AS rfind_from
FROM (SELECT CAST(value AS Utf8) AS value FROM Input);
