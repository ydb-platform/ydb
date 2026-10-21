$tupleVariantType = Variant<Int32, Int32, Int32>;

$data = [
    <|payload: 10|>,
    <|payload: 20|>,
    <|payload: 30|>,
];

SELECT
    DynamicVariant(payload, 99u, $tupleVariantType) AS variant
FROM
    as_table($data)
;
