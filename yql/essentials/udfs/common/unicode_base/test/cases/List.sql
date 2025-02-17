/* syntax version 1 */
SELECT
    value,
    Ensure(value, Unicode::JoinFromList(Unicode::SplitToList(value, "@"u), "@"u) == value) AS equals_to_original,
    Unicode::JoinFromList(Unicode::SplitToList(value, "@"u), "#"u) AS replace_delimeter,
    Unicode::SplitToList(value, "@"u) AS just_split,
    Unicode::SplitToList(value, "@"u)[0] as first,
    Unicode::SplitToList(value, "@"u, true AS SkipEmpty) AS skip_empty,
    Unicode::SplitToList(value, "b@"u, false AS DelimeterString) AS multichar_delim_set,
    Unicode::SplitToList(value, "b@"u, true AS DelimeterString) AS multichar_delim_string,
    Unicode::SplitToList(value, "@"u, 1 AS Limit) AS limited
FROM Input;
