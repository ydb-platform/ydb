/* syntax version 1 */
pragma UseBlocks;
SELECT
    value,
    String::Contains(value, "as") AS contains,
    String::HasPrefixIgnoreCase(value, "AS") AS icprefix,
    String::StartsWithIgnoreCase(value, "AS") AS icstarts,
    String::HasSuffixIgnoreCase(value, "AS") AS icsuffix,
    String::EndsWithIgnoreCase(value, "AS") AS icends,
    String::LevensteinDistance(value, "as") AS levenstein
FROM Input;
