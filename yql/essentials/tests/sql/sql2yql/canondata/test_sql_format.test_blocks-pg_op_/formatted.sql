$data = [
    <|x: 0p, y: 0p|>,
    <|x: 0p, y: 1p|>,
    <|x: 1p, y: 0p|>,
    <|x: 1p, y: 1p|>,
];

SELECT
    PgOp('=', x, y),
    PgOp('=', x, 1p)
FROM
    as_table($data)
;
