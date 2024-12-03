/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    EvaluateExpr(YQL::RandomNumber()),
    YQL::RandomNumber();

SELECT
    key,
    EvaluateExpr(CAST(CurrentUtcDate() AS string)) AS `date`
FROM Input;

SELECT
    YQL::String(EvaluateAtom("foo" || "bar"));

SELECT
    EvaluateExpr(CurrentUtcTimestamp()),
    CurrentUtcTimestamp();
$y = Yson("{a=7u;c=[<d=%true>1;#;\"привет\";-3.4]}");

SELECT
    Yson::Equals(EvaluateExpr($y), $y);

SELECT
    EvaluateExpr(TzDate("2000-01-01,Europe/Moscow"));

SELECT
    EvaluateExpr(TzDatetime("2000-01-01T01:02:03,Europe/Moscow"));

SELECT
    EvaluateExpr(TzTimestamp("2000-01-01T01:02:03.456789,Europe/Moscow"));
