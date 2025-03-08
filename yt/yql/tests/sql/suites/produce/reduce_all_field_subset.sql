USE plato;

$udf = YQL::@@
(lambda '(stream)
    (FlatMap stream
        (lambda '(pair)
            (Map (Nth pair '1) (lambda '(item) (AsStruct '('key (Nth pair '0)) '('subkey (Member item 'subkey)) )))
        )
    )
)
@@;

SELECT * FROM (
    REDUCE concat(Input1, Input2) ON key USING ALL $udf(TableRow())
)
ORDER BY key, subkey
