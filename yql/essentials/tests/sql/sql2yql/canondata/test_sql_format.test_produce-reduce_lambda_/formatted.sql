/* postgres can not */
USE plato;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Uint32 '0) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Add state item)))))
))@@;

--INSERT INTO Output
$res = (
    REDUCE Input1
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
