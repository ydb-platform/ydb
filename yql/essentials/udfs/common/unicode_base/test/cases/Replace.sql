/* syntax version 1 */
SELECT 
    value,
    Unicode::ReplaceAll(value, Utf8("аф"), Utf8("zzz")) AS all,
    Unicode::ReplaceFirst(value, Utf8("а"), Utf8("z")) AS first,
    Unicode::ReplaceLast(value, Utf8("а"), Utf8("z")) AS last,
    Unicode::ReplaceFirst(value, Utf8("а"), Utf8("")) AS first2,
    Unicode::ReplaceLast(value, Utf8("а"), Utf8("")) AS last2,
    Unicode::ReplaceFirst(value, Utf8("а"), Utf8("zzz")) AS first3,
    Unicode::ReplaceLast(value, Utf8("а"), Utf8("zzz")) AS last3
FROM Input