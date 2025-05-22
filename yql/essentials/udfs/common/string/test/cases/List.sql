/* syntax version 1 */
SELECT
    value,
    Ensure(value, String::JoinFromList(String::SplitToList(value, "@"), "@") == value) AS equals_to_original,
    String::JoinFromList(String::SplitToList(value, "@"), "#") AS replace_delimeter,
    String::SplitToList(value, "@") AS just_split,
    String::SplitToList(value, "@")[0] as first,
    String::SplitToList(value, "@", true AS SkipEmpty) AS skip_empty,
    String::SplitToList(value, "b@", false AS DelimeterString) AS multichar_delim_set,
    String::SplitToList(value, "b@", true AS DelimeterString) AS multichar_delim_string,
    String::SplitToList(value, "@", 1 AS Limit) AS limited
FROM Input;
