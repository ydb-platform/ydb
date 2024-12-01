/* syntax version 1 */
/* postgres can not */
select EvaluateCode(
    ReprCode(1));

select EvaluateCode(
    FuncCode("EvaluateCode",
    FuncCode("ReprCode", ReprCode(1))));

select EvaluateCode(
    FuncCode("EvaluateCode",
        FuncCode("ReprCode", 
            FuncCode("EvaluateCode",
                FuncCode("ReprCode", ReprCode(1))))));

select EvaluateCode(
    FuncCode("EvaluateExpr",
    QuoteCode(1 + 2)));
