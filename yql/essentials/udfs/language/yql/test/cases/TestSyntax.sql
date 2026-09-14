$t1 = YqlLang::TestSyntax("SELECT 1;");
$t2 = YqlLang::TestSyntax("SELECT");

SELECT
    $t1,
    Ensure(0, Find($t2 ?? "", "mismatched input '<EOF>'") IS NOT NULL, $t2 ?? "");
;
