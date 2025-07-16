/* syntax version 1 */
SELECT
    value,
    String::Contains(value, "as") AS contains,
    String::HasPrefix(value, "as") AS prefix,
    String::StartsWith(value, "as") AS starts,
    String::HasSuffix(value, "as") AS suffix,
    String::EndsWith(value, "as") AS ends,
    String::Find(value, "as") AS find,
    String::ReverseFind(value, "as") AS rfind,
    String::LevensteinDistance(value, "as") AS levenstein
FROM Input;
