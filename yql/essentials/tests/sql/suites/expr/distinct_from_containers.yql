/* syntax version 1 */
/* postgres can not */

$vt1 = ParseType("Variant<Int32,Int32?>");
$vt2 = ParseType("Variant<Int64,Null>");

$svt1 = ParseType("Variant<a:Int32,b:Int32?>");
$svt2 = ParseType("Variant<a:Int64,b:Null>");

select
  (1, 2) is not distinct from (1, 2, 1/0),     --true
  <|a:1/0, b:Nothing(String?), c:1|>  is not distinct from
  <|c:1u, d:1u/0u, e:Nothing(Utf8?)|>,         --true
  [1, 2, null] is not distinct from [1, 2, just(1/0)], --false
  {1:null} is distinct from {1u:2/0},          --false
  Variant(1/0, "1", $vt1) is distinct from  Variant(null, "1", $vt2), --false
  Variant(1/0, "b", $svt1) is not distinct from Variant(null, "b", $svt2), --true
;
