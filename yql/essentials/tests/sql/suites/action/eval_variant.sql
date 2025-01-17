/* syntax version 1 */
/* postgres can not */
$vt = ParseType("Variant<One:Int32,Two:String>");
SELECT Variant(12, CAST(Unicode::ToUpper("o"u) AS String) || "ne", $vt);
