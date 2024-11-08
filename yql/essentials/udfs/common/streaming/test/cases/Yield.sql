/* syntax version 1 */
SELECT YQL::@@(block '(
    (let vt (VariantType (TupleType (DataType 'String) (DataType 'String))))
	(let inputRows (AsList
		(Variant (String 'abbbbd111) '1 vt)
		(Variant (String 'btzzzzzzzzzz) '0 vt)
		(Variant (String 'kaziiaakkakaka) '1 vt)
		(Variant (String 'bufffffffff) '0 vt)
		(Variant (String 'aaaaa11111qqqqd) '1 vt)
		(Variant (String 'zoppppppppp) '0 vt)
		(Variant (String 'arrrrrrrr) '0 vt)
		(Variant (String 'zzzzzzzzzzzzzzz) '0 vt)
		(Variant (String 'wwwwwwwwwwwwwww1) '0 vt)
		(Variant (String 'baaaaaaaaaaaaaaa) '1 vt)
	))

    (let udf (Udf '"Streaming.Process"))
    (let args1 (AsList (String '"[ab1]")))
    (let args2 (AsList (String '"[rpd]")))

    (let pr (lambda '(x) (block '(
         (let res (AsStruct '('Data x)))
         (return res)
         ))))

    (let tr1 (lambda '(x) (block '(
        (let y (OrderedMap x pr))
        (return (Apply udf y (String '"grep") args1)))
    )))

    (let tr2 (lambda '(x) (block '(
        (let y (OrderedMap x pr))
        (return (Apply udf y (String '"grep") args2)))
    )))

    (let id (lambda '(x) x))
    (let res1 (Switch (Iterator inputRows (DependsOn (String 'A))) '1 '('0) tr1 '('1) tr2))
    (let pr2 (lambda '(x) (Member x 'Data)))
    (let pr3 (lambda '(x) (Visit x '0 pr2 '1 pr2)))
    (let res2 (OrderedMap (Collect res1) pr3))
    (let res3 (Sort res2 (Bool 'true) id))

    (return res3)
))@@;
