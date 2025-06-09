/* syntax version 1 */
SELECT
    value,
    String::HasPrefixIgnoreCase(value, "AS") AS icprefix,
    String::StartsWithIgnoreCase(value, "AS") AS icstarts,
    String::HasSuffixIgnoreCase(value, "AS") AS icsuffix,
    String::EndsWithIgnoreCase(value, "AS") AS icends,
FROM Input;
