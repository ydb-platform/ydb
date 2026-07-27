$tupleVariantType = Variant<Int32, String>;

$data = [
    <|variantValue: Variant(1, '0', $tupleVariantType)|>,
    <|variantValue: Variant('hello', '1', $tupleVariantType)|>,
    <|variantValue: Variant(42, '0', $tupleVariantType)|>,
];

SELECT
    Way(variantValue) AS way
FROM
    as_table($data)
;
