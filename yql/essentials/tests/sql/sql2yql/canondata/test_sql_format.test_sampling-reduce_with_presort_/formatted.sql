/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 8 */
USE plato;
$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Uint32 '0) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Add state item)))))
))@@;

$res = (
    REDUCE Input
    TABLESAMPLE BERNOULLI (30) REPEATABLE (1)
    PRESORT
        key || subkey
    ON
        key
    USING $udf(CAST(value AS uint32) ?? 0)
);

SELECT
    *
FROM
    $res
ORDER BY
    key
;
