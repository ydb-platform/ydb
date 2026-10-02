$structVariantType = Variant<a: Int32, b: String>;

$optionalData = [
    <|variantValue: Just(Variant(1, 'a', $structVariantType))|>,
    <|variantValue: Nothing(OptionalType($structVariantType))|>,
    <|variantValue: Just(Variant('world', 'b', $structVariantType))|>,
];

SELECT
    Way(variantValue) AS way
FROM
    as_table($optionalData)
;
