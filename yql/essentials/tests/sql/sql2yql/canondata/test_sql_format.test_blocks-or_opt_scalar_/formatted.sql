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
    Opaque(FALSE) OR y,
    Opaque(TRUE) OR y,
    Opaque(Nothing(bool?)) OR y
FROM
    as_table($data)
;

SELECT
    x,
    y,
    x OR Opaque(FALSE),
    x OR Opaque(TRUE),
    x OR Opaque(Nothing(bool?))
FROM
    as_table($data)
;
