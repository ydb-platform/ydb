$x = ($type) -> (FormatType($type));

select Substring($x(String), 1);
select EvaluateExpr($x(String));
