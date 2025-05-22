/* syntax version 1 */
/* postgres can not */
SELECT EnsureType("a", String, CAST(Unicode::ToUpper("me"u) AS String) || "ssage");
