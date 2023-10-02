/* syntax version 1 */
/* postgres can not */

$udf = YQL::@@
(lambda '(stream)
    (PartitionByKey stream
        (lambda '(item) (Way item))
        (Void)
        (Void)
        (lambda '(listOfPairs)
            (FlatMap listOfPairs (lambda '(pair)
                (Map (Nth pair '1) (lambda '(elem)
                    (AsStruct
                        '('cnt (Visit elem '0 (lambda '(v) (Member v 'subkey)) '1 (lambda '(v) (Member v 'subkey))))
                        '('src (Nth pair '0))
                    )
                ))
            ))
        )
    )
)
@@;

INSERT INTO plato.Output WITH TRUNCATE
PROCESS plato.Input0, (select * from plato.Input0 where key > "100") USING $udf(TableRows());
