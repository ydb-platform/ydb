/* postgres can not */
USE plato;
PRAGMA yt.UseNativeDescSort;
$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Nothing (OptionalType (DataType 'String))) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Coalesce state (Just item))))))
))@@;

SELECT
    *
FROM (
    REDUCE Input1
    PRESORT
        value DESC
    ON
        key,
        subkey
    USING $udf(value) --YtReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE Input1
    PRESORT
        subkey DESC,
        value DESC
    ON
        key
    USING $udf(value) --YtReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE Input1
    PRESORT
        value
    ON
        key,
        subkey
    USING $udf(value) --YtMapReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE concat(Input1, Input2)
    PRESORT
        value DESC
    ON
        key,
        subkey
    USING $udf(value) --YtMapReduce
)
ORDER BY
    key,
    summ
;
