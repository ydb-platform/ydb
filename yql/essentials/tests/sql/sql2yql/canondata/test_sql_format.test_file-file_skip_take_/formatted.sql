$file = ParseFile('String', 'keyid.lst');

$file = ListTake(ListSkip($file, 3ul), 2ul);

$file = EvaluateExpr($file);

SELECT
    $file
;
