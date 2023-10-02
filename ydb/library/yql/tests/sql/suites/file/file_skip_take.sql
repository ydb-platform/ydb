/* postgres can not */
/* syntax version 1 */
$file = ParseFile("String", "keyid.lst");

$file = ListTake(ListSkip($file, 3ul), 2ul);

$file = EvaluateExpr($file);

select $file;

