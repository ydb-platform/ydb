$x = ($type) -> (FormatType($type));

SELECT
    Substring($x(String), 1)
;

SELECT
    EvaluateExpr($x(String))
;
