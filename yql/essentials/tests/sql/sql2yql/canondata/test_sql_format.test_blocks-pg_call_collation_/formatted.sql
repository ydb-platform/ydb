$data = [
    <|x: "straße"p|>,
    <|x: "foo"p|>,
];

SELECT
    PgCall('upper', x, 'de-DE-x-icu' AS Collation),
    PgCall('upper', x)
FROM
    as_table($data)
;
