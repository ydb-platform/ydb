$structVariantType = Variant<a: Optional<Int32>, b: Optional<Int32>>;

$optionalData = [
    <|variantValue: Just(Variant(Just(1), 'a', $structVariantType))|>,
    <|variantValue: Just(Variant(Nothing(Int32?), 'a', $structVariantType))|>,
    <|variantValue: Nothing(OptionalType($structVariantType))|>,
    <|variantValue: Just(Variant(Just(2), 'b', $structVariantType))|>,
    <|variantValue: Just(Variant(Nothing(Int32?), 'b', $structVariantType))|>,
];

SELECT
    Yql::VariantItem(variantValue) AS item
FROM
    as_table($optionalData)
;
