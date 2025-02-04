$data = [
    <|x: FALSE, y: 1, z: 2|>,
    <|x: TRUE, y: 3, z: 4|>,
];

SELECT
    if(x, y, z)
FROM
    as_table($data)
;
