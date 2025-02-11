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
    Opaque(FALSE) AND y,
    Opaque(TRUE) AND y,
    Opaque(Nothing(bool?)) AND y
FROM
    as_table($data)
;

SELECT
    x,
    y,
    x AND Opaque(FALSE),
    x AND Opaque(TRUE),
    x AND Opaque(Nothing(bool?))
FROM
    as_table($data)
;
