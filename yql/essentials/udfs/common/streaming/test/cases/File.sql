/* syntax version 1 */
SELECT YQL::@@(block '(
	(let x (Read! world (DataSource '"yt" '"plato") (Key '('table (String '"Input"))) (Void) '()))

  	(let world (Left! x))
 	(let table0 (Right! x))

 	(let data (FlatMap table0 (lambda '(row) (block '(
        (let res (Struct))
        (let res (AddMember res '"Data" ("Apply" ("Udf" '"String.JoinFromList") ("AsList" (Member row '"key") (Member row '"subkey") (Member row '"value")) (String '","))))
        (let res (AsList res))
        (return res)
        ))))
    )

	(let udf (Udf '"Streaming.Process"))
	(let args1 (List (ListType (DataType 'String)) (String '"[13]")))
	(let res1 (LMap data (lambda '(stream) (Apply udf stream (String '"grep") args1))))

	(let args2 (List (ListType (DataType 'String)) (String '"2")))
	(let res2 (LMap res1 (lambda '(stream) (Apply udf stream (String '"grep") args2))))

	(return res2)
))@@;
