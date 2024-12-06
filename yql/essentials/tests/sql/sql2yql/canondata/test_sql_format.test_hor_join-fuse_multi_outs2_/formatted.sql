USE plato;
$udf = YQL::@@(lambda '(flow) 
(Map flow
   (lambda '(item) (block '(
        (let varTuple (VariantType (TupleType
            (StructType
                '('key (DataType 'String))
                '('subkey (DataType 'String))
                '('value (DataType 'String))
            )
            (StructType
                '('key (DataType 'String))
                '('subkey (DataType 'String))
                '('value (DataType 'String))
            )
        )))
        (let intValue (FromString (Member item 'key) 'Int32))
        (let res
            (If (Coalesce (Equal (% intValue (Int32 '2)) (Int32 '0)) (Bool 'false))
                (Variant item '0 varTuple)
                (Variant item '1 varTuple)
            )
        )
        (return res)
    )))
))@@;

$i, $j = (
    PROCESS Input
    USING $udf(TableRows())
);

SELECT
    key,
    value
FROM
    $i
WHERE
    key > "100"
ORDER BY
    key
;

INSERT INTO @a
SELECT
    *
FROM
    $j
;

INSERT INTO @b
SELECT
    key
FROM
    $j
WHERE
    key > "200"
;
