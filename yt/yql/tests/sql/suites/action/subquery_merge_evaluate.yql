/* syntax version 1 */
/* postgres can not */
use plato;

define subquery $sub1($i,$j?) as
   select $i - 1,$j;
end define;

define subquery $sub2($i,$j?) as
   select $i + 1,$j;
end define;

$sub = EvaluateCode(If(1>2,QuoteCode($sub1),QuoteCode($sub2)));

$s = SubqueryExtendFor([1,2,3],$sub);
process $s();
