/* syntax version 1 */
/* postgres can not */
USE plato;

$udf = YQL::@@
(lambda '(key stream)
    (PartitionByKey stream
        (lambda '(item) (Way item))
        (Void)
        (Void)
        (lambda '(listOfPairs)
            (FlatMap listOfPairs
                (lambda '(pair) (Just (AsStruct '('key key) '('src (Nth pair '0)) '('cnt (Length (ForwardList (Nth pair '1)))))))
            )
        )
    )
)
@@;

$r = (REDUCE Input, AS_TABLE(ListMap(ListFromRange(0,10), ($val) -> {
    RETURN AsStruct(Cast($val AS String) AS key, Cast($val AS String) AS subkey, Cast($val AS String) AS value)
})) ON key USING $udf(TableRow()));

SELECT key, src, cnt FROM $r ORDER BY key, src, cnt;