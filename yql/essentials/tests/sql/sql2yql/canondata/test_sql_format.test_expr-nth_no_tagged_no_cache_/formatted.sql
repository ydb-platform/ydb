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
FROM
    AS_TABLE($data)
;

SELECT
    x.1,
FROM
    AS_TABLE($data)
;

SELECT
    x.2,
FROM
    AS_TABLE($data)
;

SELECT
    x.3,
FROM
    AS_TABLE($data)
;

SELECT
    x.4,
FROM
    AS_TABLE($data)
;
