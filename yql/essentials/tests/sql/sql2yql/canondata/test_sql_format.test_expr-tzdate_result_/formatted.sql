/* syntax version 1 */
/* postgres can not */
$parse = DateTime::Parse('%Y-%m-%d,%Z');
$date_string = '2020-05-30,Europe/Moscow';
$date = $parse($date_string);

SELECT
    DateTime::MakeTzDate($date),
    DateTime::MakeTzDatetime($date)
;

SELECT
    EvaluateExpr(DateTime::MakeTzDate($date))
;

$parse = DateTime::Parse('%Y-%m-%d,%Z');
$date_string = '2020-05-30,America/Los_Angeles';
$date = $parse($date_string);

SELECT
    DateTime::MakeTzDate($date),
    DateTime::MakeTzDatetime($date)
;

SELECT
    EvaluateExpr(DateTime::MakeTzDate($date))
;
