$data = [
    <|x: FALSE, y: 1, z: 2|>,
    <|x: TRUE, y: 3, z: 4|>,
];

SELECT
    x,
    if(Opaque(FALSE), y, z),
    if(Opaque(TRUE), y, z),
    if(x, Opaque(5), z),
    if(x, 5, Opaque(6))
FROM
    as_table($data)
;

SELECT
    x,
    if(Opaque(FALSE), Opaque(5), z),
    if(Opaque(TRUE), y, Opaque(6))
FROM
    as_table($data)
;
