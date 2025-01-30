$data = [
    <|x: FALSE, y: FALSE|>,
    <|x: FALSE, y: TRUE|>,
    <|x: TRUE, y: FALSE|>,
    <|x: TRUE, y: TRUE|>,
];

SELECT
    x,
    y,
    x XOR Opaque(FALSE),
    x XOR Opaque(TRUE)
FROM
    as_table($data)
;

SELECT
    x,
    y,
    Opaque(FALSE) XOR y,
    Opaque(TRUE) XOR y
FROM
    as_table($data)
;
