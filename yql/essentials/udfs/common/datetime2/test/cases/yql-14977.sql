/* syntax version 1 */
$parse = DateTime::Parse("%B/%d/%Y");
$format = DateTime::Format("%b/%d/%Y");

select $format($parse("mAy/15/2022"));

