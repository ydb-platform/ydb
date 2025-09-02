$data = [
    <|x: FALSE, y: FALSE|>,
    <|x: FALSE, y: TRUE|>,
    <|x: TRUE, y: FALSE|>,
    <|x: TRUE, y: TRUE|>,
];

SELECT
    x,
    y,
    x OR Opaque(FALSE),
    x OR Opaque(TRUE)
FROM
    as_table($data)
;

SELECT
    x,
    y,
    Opaque(FALSE) OR y,
    Opaque(TRUE) OR y
FROM
    as_table($data)
;
