/* syntax version 1 */
/* postgres can not */
SELECT
    EvaluateCode(
        ReprCode(1)
    )
;

SELECT
    EvaluateCode(
        FuncCode(
            'EvaluateCode',
            FuncCode('ReprCode', ReprCode(1))
        )
    )
;

SELECT
    EvaluateCode(
        FuncCode(
            'EvaluateCode',
            FuncCode(
                'ReprCode',
                FuncCode(
                    'EvaluateCode',
                    FuncCode('ReprCode', ReprCode(1))
                )
            )
        )
    )
;

SELECT
    EvaluateCode(
        FuncCode(
            'EvaluateExpr',
            QuoteCode(1 + 2)
        )
    )
;
