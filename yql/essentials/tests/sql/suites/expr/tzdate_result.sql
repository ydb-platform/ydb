/* syntax version 1 */
/* postgres can not */
$parse = DateTime::Parse("%Y-%m-%d,%Z");

$date_string = '2020-05-30,Europe/Moscow';
$date = $parse($date_string);

select 
DateTime::MakeTzDate($date),
DateTime::MakeTzDatetime($date);

select EvaluateExpr(DateTime::MakeTzDate($date));

$parse = DateTime::Parse("%Y-%m-%d,%Z");

$date_string = '2020-05-30,America/Los_Angeles';
$date = $parse($date_string);

select 
DateTime::MakeTzDate($date),
DateTime::MakeTzDatetime($date);

select EvaluateExpr(DateTime::MakeTzDate($date));
