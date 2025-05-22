/* syntax version 1 */
/* postgres can not */
$fact = EvaluateCode(
    Yql::Fold(
        ListFromRange(1,11),
        ReprCode(1),
        ($item, $state)->{ 
            return FuncCode("*", $state, ReprCode($item)) 
        })
);

select $fact;
