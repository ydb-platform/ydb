$structVariantType = Variant<a: Int32, b: Int32>;

$data = [
    <|variantValue: Variant(1, 'a', $structVariantType)|>,
    <|variantValue: Variant(2, 'b', $structVariantType)|>,
];

SELECT
    Yql::VariantItem(variantValue) AS item
FROM
    as_table($data)
;
