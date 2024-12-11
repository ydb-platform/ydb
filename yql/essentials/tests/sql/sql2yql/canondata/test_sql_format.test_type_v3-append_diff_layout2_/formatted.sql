/* multirun can not */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "complex";

$udf = YQL::@@(lambda '(x) (block '(
    (let structType (StructType '('key (DataType 'String)) '('subkey (StructType '('a (DataType 'String)) '('b (OptionalType (DataType 'Int32))) '('c (DataType 'String))))))
    (let varType (VariantType (TupleType structType structType structType)))
    (let res (Map x
        (lambda '(r)
            (If
                (Coalesce (> (SafeCast (Member r 'key) (DataType 'Int32)) (Int32 '200)) (Bool 'false))
                (Variant r '0 varType)
                (If
                    (Coalesce (< (SafeCast (Member r 'key) (DataType 'Int32)) (Int32 '50)) (Bool 'false))
                    (Variant r '1 varType)
                    (Variant r '2 varType)
                )
            )
        )
    ))
    (return res)
)))@@;

$i, $j, $k = (
    PROCESS Input
    USING $udf(TableRows())
);

INSERT INTO Output1
SELECT
    *
FROM
    $i
;

INSERT INTO Output2
SELECT
    *
FROM
    $j
LIMIT 2;

INSERT INTO Output3
SELECT
    *
FROM
    $k
;

INSERT INTO Output3
SELECT
    *
FROM
    $j
;
