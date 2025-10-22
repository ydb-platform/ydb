/* postgres can not */
PRAGMA EmitAggApply;
PRAGMA EmitTableSource;

$data = [
    <|
        x: Just(
            (
                1,
                just(2),
                NULL,
                Nothing(Int32?),
                Nothing(pgint4)
            )
        )
    |>,
    <|x: NULL|>
];

SELECT
    x.0,
    x.1,
    x.2,
    x.3,
    x.4
FROM
    AS_TABLE($data)
;
