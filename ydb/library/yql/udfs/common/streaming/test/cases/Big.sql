/* syntax version 1 */
SELECT YQL::@@(block '(
    (let times16 (lambda '(x) (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x (Concat x x)))))))))))))))))
    (let s2_8 (Apply times16 (String '"1000000000000007")))
    (let s2_12 (Apply times16 s2_8))
    (let s2_16 (Apply times16 s2_12))
    (let s2_20 (Apply times16 s2_16))
    (let s2_24 (Apply times16 s2_20))

    (let s2_12_1 (Concat s2_12 (String '"762")))
    (let vt (VariantType (TupleType (DataType 'Int32) (DataType 'String))))
	(let inputRows (AsList
		(Variant s2_24 '1 vt)
		(Variant s2_8 '1 vt)
		(Variant s2_12 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_16 '1 vt)
		(Variant s2_8 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_24 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_24 '1 vt)
		(Variant s2_24 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_24 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_12_1 '1 vt)
		(Variant s2_16 '1 vt)
	))

    (let udf (Udf '"Streaming.Process"))

    (let pr (lambda '(x) (block '(
         (let res (AsStruct '('Data x)))
         (return res)
         ))))

    (let tr1 (lambda '(x) (block '(
        (let y (OrderedMap x pr))
        (return (Apply udf y (String '"cat"))))
    )))

    (let hugeResult (Switch (Iterator inputRows (DependsOn (String 'A))) '1 '('1) tr1))
    (let md5Udf (Udf '"Digest.Md5Hex"))
    (let shortResult (OrderedMap hugeResult (lambda '(x) (Apply md5Udf (Member x 'Data)))))
    (return (Collect shortResult))
))@@;
