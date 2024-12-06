USE plato;

INSERT INTO @skv1v2
SELECT
    key,
    subkey,
    value AS value1,
    value AS value2
FROM
    Input
ORDER BY
    subkey,
    key,
    value1,
    value2
;

INSERT INTO @skv2v1
SELECT
    key,
    subkey,
    value AS value1,
    value AS value2
FROM
    Input
ORDER BY
    subkey,
    key,
    value2,
    value1
;

INSERT INTO @ksv1v2
SELECT
    key,
    subkey,
    value AS value1,
    value AS value2
FROM
    Input
ORDER BY
    key,
    subkey,
    value1,
    value2
;
COMMIT;
$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Nothing (OptionalType (DataType 'String))) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Coalesce state (Just item))))))
))@@;

SELECT
    *
FROM (
    REDUCE concat(@skv1v2, @skv1v2)
    PRESORT
        value1,
        value2
    ON
        key,
        subkey
    USING $udf(value1) --YtReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE @ksv1v2
    PRESORT
        value2,
        value1
    ON
        key,
        subkey
    USING $udf(value1) --YtMapReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE concat(@skv1v2, @skv2v1)
    PRESORT
        value1,
        value2
    ON
        key,
        subkey
    USING $udf(value1) --YtMapReduce
)
ORDER BY
    key,
    summ
;

SELECT
    *
FROM (
    REDUCE concat(@skv1v2, @ksv1v2)
    PRESORT
        value1,
        value2
    ON
        key,
        subkey
    USING $udf(value1) --YtMapReduce
)
ORDER BY
    key,
    summ
;
