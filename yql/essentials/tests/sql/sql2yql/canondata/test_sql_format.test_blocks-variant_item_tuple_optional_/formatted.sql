$tupleVariantType = Variant<Int32, Int32>;

$optionalData = [
    <|variantValue: Just(Variant(1, '0', $tupleVariantType))|>,
    <|variantValue: Nothing(OptionalType($tupleVariantType))|>,
    <|variantValue: Just(Variant(2, '1', $tupleVariantType))|>,
];

SELECT
    Yql::VariantItem(variantValue) AS item
FROM
    as_table($optionalData)
;
