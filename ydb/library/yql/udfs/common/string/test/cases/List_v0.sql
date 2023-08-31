/* syntax version 1 */
-- use SplitToList settings which are used as defaults in v0 syntax 
SELECT
    value,
    IF (
        String::Contains(value, "@@"),
        Ensure(
            value, 
            String::JoinFromList(String::SplitToList(value, "@", true AS SkipEmpty, false AS DelimeterString), "@") != value,
            value
        )
    ) AS not_equals_to_original,
    IF (
        String::Contains(value, "@@"),
        Ensure(
            value, 
            String::JoinFromList(String::SplitToList(value, "@", true AS SkipEmpty, false AS DelimeterString), "@") != value,
            value
        )
    ) AS not_equals_to_original_skip_empty,
    Ensure(
        value, 
        String::JoinFromList(String::SplitToList(value, "@", false AS SkipEmpty, false AS DelimeterString), "@") == value,
        value
    ) AS equals_to_original,
    String::SplitToList(value, "@#", true AS SkipEmpty, false AS DelimeterString) AS multichar
FROM Input;
