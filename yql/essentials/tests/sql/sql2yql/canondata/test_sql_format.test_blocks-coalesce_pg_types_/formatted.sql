$data = [
    <|x: 0p, y: 0p|>,
    <|x: 0p, y: 1p|>,
    <|x: NULL, y: 0p|>,
    <|x: 1p, y: 1p|>,
];

SELECT
    Coalesce(x, y)
FROM
    as_table($data)
;
