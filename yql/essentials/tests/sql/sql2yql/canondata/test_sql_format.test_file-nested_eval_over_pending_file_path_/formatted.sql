$p = 'http_test://' || 'foo.txt';

PRAGMA file('foo.txt', $p);

$e1 = EvaluateExpr(FileContent('foo.txt'));
$e2 = EvaluateExpr($e1);

SELECT
    $e1,
    $e2
;
