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
    x AND y
FROM
    as_table($data)
;
