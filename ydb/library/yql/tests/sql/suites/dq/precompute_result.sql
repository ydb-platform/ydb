use plato;

$l = select min(cast(key as uint64)) as key from Input;
$l = EvaluateExpr($l);

select * from Input limit $l ?? 0;
