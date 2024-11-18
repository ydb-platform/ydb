/* syntax version 1 */
SELECT 
    CAST(value AS Utf8),
    Unicode::RemoveAll(CAST(value AS Utf8), Utf8("фа")) AS all,
    Unicode::RemoveFirst(CAST(value AS Utf8), Utf8("а")) AS first,
    Unicode::RemoveLast(CAST(value AS Utf8), Utf8("а")) AS last,
    Unicode::RemoveFirst(CAST(value AS Utf8), Utf8("фа")) AS first2,
    Unicode::RemoveLast(CAST(value AS Utf8), Utf8("фа")) AS last2
FROM Input;
