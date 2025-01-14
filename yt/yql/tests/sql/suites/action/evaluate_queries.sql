/* syntax version 1 */
/* postgres can not */
USE plato;

$s = (select count(*) from Input);
select EvaluateExpr($s);
/*
$s = (select key from Input);
select EvaluateExpr($s);

$s = (select key || "foo" from Input);
select EvaluateExpr($s);

$s = (select aggregate_list(key) from Input);
select EvaluateExpr($s);

$s = (select v from (select key,min(value) as v from Input group by key order by key limit 1));
select EvaluateExpr($s);

$s = (select key from Input order by key limit 1);
select EvaluateExpr($s);

$s = (select lag(key) over w from Input window w as ());
select EvaluateExpr($s);

$s = (select a.key from Input as a join Input as b using(key));
select EvaluateExpr($s);
*/
