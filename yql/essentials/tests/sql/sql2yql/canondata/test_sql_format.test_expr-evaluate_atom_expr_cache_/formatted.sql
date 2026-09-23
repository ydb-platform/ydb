PRAGMA EvaluateExprCache;

SELECT
    Yql::String(EvaluateAtom(EvaluateExpr('x')))
;
