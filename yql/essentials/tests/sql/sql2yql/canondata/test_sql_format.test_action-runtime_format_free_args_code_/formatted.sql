$x = EvaluateCode(
    LambdaCode(
        ($arg1, $arg2) -> {
            $f = FuncCode('Concat', $arg1, $arg2);
            RETURN ReprCode(FormatCode($f));
        }
    )
);

SELECT
    $x(1, 2)
;
