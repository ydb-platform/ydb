$structVariantType = Variant<a: Int32, b: String>;

$data = [
    <|variantValue: Variant(1, 'a', $structVariantType)|>,
    <|variantValue: Variant('world', 'b', $structVariantType)|>,
];

SELECT
    variantValue,
    variantValue.a AS aAlternative,
    variantValue.b AS bAlternative
FROM
    as_table($data)
;
