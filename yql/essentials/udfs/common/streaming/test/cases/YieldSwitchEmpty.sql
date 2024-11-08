/* syntax version 1 */
SELECT YQL::@@(block '(
    (let vt (VariantType (TupleType (DataType 'String) (DataType 'String))))
	(let inputRows (AsList
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
		(Variant (String 'aaaaaa) '0 vt)
		(Variant (String 'bbbbbb) '1 vt)
	))

    (let udf (Udf '"Streaming.Process"))
    (let args (AsList (String '"-c") (String '"grep missing || true")))

    (let pr (lambda '(x) (block '(
         (let res (AsStruct '('Data x)))
         (return res)
         ))))

    (let tr1 (lambda '(x) (block '(
        (let y (Map x pr))
        (return (Apply udf y (String '"bash") args)))
    )))

    (let tr2 (lambda '(x) (block '(
        (let y (Map x pr))
        (return (Apply udf y (String '"bash") args)))
    )))

    (let input2 (Switch (Iterator inputRows (DependsOn (String 'A))) '1 '('0) tr1 '('1) tr2))

    (return (Collect input2))
))@@;
