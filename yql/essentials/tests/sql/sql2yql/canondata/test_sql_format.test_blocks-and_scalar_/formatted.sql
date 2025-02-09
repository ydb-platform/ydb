$data = [
    <|x: FALSE, y: FALSE|>,
    <|x: FALSE, y: TRUE|>,
    <|x: TRUE, y: FALSE|>,
    <|x: TRUE, y: TRUE|>,
];

SELECT
    x,
    y,
    x AND Opaque(FALSE),
    x AND Opaque(TRUE)
FROM
    as_table($data)
;

SELECT
    x,
    y,
    Opaque(FALSE) AND y,
    Opaque(TRUE) AND y
FROM
    as_table($data)
;
