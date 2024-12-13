/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 16 */
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

$r = (REDUCE Input SAMPLE(0.1), Input SAMPLE(0.1) ON key USING $udf(TableRow()));

SELECT key, src, cnt FROM $r ORDER BY key, src, cnt;
