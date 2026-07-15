SELECT
    EvaluateExpr(pgfloat8('NaN')),
    EvaluateExpr(pgfloat8('Inf')),
    EvaluateExpr(pgfloat8('-Inf')),
    EvaluateExpr(pgfloat4('NaN')),
    EvaluateExpr(pgfloat4('Inf')),
    EvaluateExpr(pgfloat4('-Inf'))
;
