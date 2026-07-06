$tupleVariantType = Variant<Int32, String>;

$data = [
    <|variantValue: Variant(1, '0', $tupleVariantType)|>,
    <|variantValue: Variant('hello', '1', $tupleVariantType)|>,
    <|variantValue: Variant(42, '0', $tupleVariantType)|>,
];

SELECT
    variantValue.0 AS intAlternative,
    variantValue.1 AS strAlternative
FROM
    as_table($data)
;
