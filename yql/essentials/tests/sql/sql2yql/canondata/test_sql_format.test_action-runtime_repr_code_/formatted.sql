/* syntax version 1 */
/* postgres can not */
$fact = EvaluateCode(
    Yql::Fold(
        ListFromRange(1, 11),
        ReprCode(1),
        ($item, $state) -> {
            RETURN FuncCode('*', $state, ReprCode($item));
        }
    )
);

SELECT
    $fact
;
