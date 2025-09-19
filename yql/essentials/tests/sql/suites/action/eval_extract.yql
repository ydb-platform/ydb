/* syntax version 1 */
/* postgres can not */
$list = AsList(AsStruct(1 as One, "2" as Two), AsStruct(3 as One, "4" as Two));
SELECT ListExtract($list, CAST(Unicode::ToUpper("o"u) AS String) || "ne");
