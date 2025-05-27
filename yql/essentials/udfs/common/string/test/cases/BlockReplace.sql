/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    value,
    String::ReplaceAll(value, "as", "zzz") AS all,
    String::ReplaceFirst(value, "a", "z") AS first,
    String::ReplaceLast(value, "a", "z") AS last,
    String::ReplaceFirst(value, "a", "zz") AS first2,
    String::ReplaceLast(value, "a", "zz") AS last2,
    String::ReplaceFirst(value, "a", "") AS first3,
    String::ReplaceLast(value, "a", "") AS last3
FROM Input;
