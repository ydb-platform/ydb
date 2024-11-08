/* syntax version 1 */
SELECT 
    CAST(value AS Utf8),
    Unicode::ReplaceAll(CAST(value AS Utf8), Utf8("аф"), Utf8("zzz")) AS all,
    Unicode::ReplaceFirst(CAST(value AS Utf8), Utf8("а"), Utf8("z")) AS first,
    Unicode::ReplaceLast(CAST(value AS Utf8), Utf8("а"), Utf8("z")) AS last,
    Unicode::ReplaceFirst(CAST(value AS Utf8), Utf8("а"), Utf8("")) AS first2,
    Unicode::ReplaceLast(CAST(value AS Utf8), Utf8("а"), Utf8("")) AS last2,
    Unicode::ReplaceFirst(CAST(value AS Utf8), Utf8("а"), Utf8("zzz")) AS first3,
    Unicode::ReplaceLast(CAST(value AS Utf8), Utf8("а"), Utf8("zzz")) AS last3
FROM Input;
