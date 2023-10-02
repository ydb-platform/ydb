/* syntax version 1 */
/* postgres can not */
/* multirun can not */
USE plato;

$s1 = (select count(*) from Output with xlock);
$s2 = (select max(key) from Output with xlock);
insert into Output with truncate
select EvaluateExpr($s1) as a, EvaluateExpr($s2) as b;
