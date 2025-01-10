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
                (lambda '(pair) (Just (AsStruct '('key (Nth key '0)) '('src (Nth pair '0)) '('cnt (Length (ForwardList (Nth pair '1)))))))
            )
        )
    )
)
@@;

$r = (REDUCE Input, Input ON key,subkey USING $udf(TableRow()));

SELECT key, src, cnt FROM $r ORDER BY key, src, cnt;
