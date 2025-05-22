/* syntax version 1 */
/* postgres can not */
use plato;

$combineQueries = ($query, $list) -> {
    RETURN EvaluateCode(LambdaCode(($world) -> {
        $queries = ListMap($list, ($arg)->{
                RETURN FuncCode("Apply", QuoteCode($query), $world, ReprCode($arg))
            });

        RETURN FuncCode("UnionAll", $queries);
    }));
};

DEFINE SUBQUERY $calc($table) AS
    SELECT *
    FROM $table;
END DEFINE;

$fullQuery = $combineQueries($calc, AsList("Input", "Input"));

SELECT count(*) FROM $fullQuery();
