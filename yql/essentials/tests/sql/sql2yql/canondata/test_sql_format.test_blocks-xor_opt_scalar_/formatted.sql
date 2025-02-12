$data = [
    <|x: FALSE, y: FALSE|>,
    <|x: FALSE, y: TRUE|>,
    <|x: FALSE, y: NULL|>,
    <|x: TRUE, y: FALSE|>,
    <|x: TRUE, y: TRUE|>,
    <|x: TRUE, y: NULL|>,
    <|x: NULL, y: FALSE|>,
    <|x: NULL, y: TRUE|>,
    <|x: NULL, y: NULL|>,
];

SELECT
    x,
    y,
    Opaque(FALSE) XOR y,
    Opaque(TRUE) XOR y,
    Opaque(Nothing(bool?)) XOR y
FROM
    as_table($data)
;

SELECT
    x,
    y,
    x XOR Opaque(FALSE),
    x XOR Opaque(TRUE),
    x XOR Opaque(Nothing(bool?))
FROM
    as_table($data)
;
