$data = [
    <|x: 0p, y: 0p|>,
    <|x: 0p, y: 1p|>,
    <|x: 1p, y: 0p|>,
    <|x: 1p, y: 1p|>,
];

SELECT
    PgCall('int4pl', x, y),
    PgCall('int4pl', x, 1p)
FROM
    as_table($data)
;
