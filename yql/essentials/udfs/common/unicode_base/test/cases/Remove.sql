/* syntax version 1 */
SELECT 
    value as value,
    Unicode::RemoveAll(value, "фа"u) AS all,
    Unicode::RemoveFirst(value, "а"u) AS first,
    Unicode::RemoveLast(value, "а"u) AS last,
    Unicode::RemoveFirst(value, "фа"u) AS first2,
    Unicode::RemoveLast(value, "фа"u) AS last2
FROM Input;
