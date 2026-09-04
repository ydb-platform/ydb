$structVariantType = Variant<a: Int32, b: Int32>;

$optionalData = [
    <|variantValue: Just(Variant(1, 'a', $structVariantType))|>,
    <|variantValue: Nothing(OptionalType($structVariantType))|>,
    <|variantValue: Just(Variant(2, 'b', $structVariantType))|>,
];

SELECT
    Yql::VariantItem(variantValue) AS item
FROM
    as_table($optionalData)
;
