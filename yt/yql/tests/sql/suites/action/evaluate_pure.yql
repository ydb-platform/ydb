/* syntax version 1 */
/* postgres can not */
USE plato;

select EvaluateExpr(YQL::RandomNumber()), YQL::RandomNumber();
select key, EvaluateExpr(cast(CurrentUtcDate() as string)) as `date` from Input;
select YQL::String(EvaluateAtom("foo" || "bar"));
select EvaluateExpr(CurrentUtcTimestamp()), CurrentUtcTimestamp();
$y = Yson("{a=7u;c=[<d=%true>1;#;\"привет\";-3.4]}");
select Yson::Equals(EvaluateExpr($y), $y);
select EvaluateExpr(TzDate("2000-01-01,Europe/Moscow"));
select EvaluateExpr(TzDatetime("2000-01-01T01:02:03,Europe/Moscow"));
select EvaluateExpr(TzTimestamp("2000-01-01T01:02:03.456789,Europe/Moscow"));
