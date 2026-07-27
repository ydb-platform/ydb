$structVariantType = Variant<a: Int32, b: String>;

$data = [
    <|variantValue: Variant(1, 'a', $structVariantType)|>,
    <|variantValue: Variant('world', 'b', $structVariantType)|>,
];

SELECT
    Way(variantValue) AS way
FROM
    as_table($data)
;
