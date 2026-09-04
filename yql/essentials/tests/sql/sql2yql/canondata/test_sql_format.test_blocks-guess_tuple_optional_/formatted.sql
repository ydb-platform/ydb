$tupleVariantType = Variant<Int32, String>;

$optionalData = [
    <|variantValue: Just(Variant(1, '0', $tupleVariantType))|>,
    <|variantValue: Nothing(OptionalType($tupleVariantType))|>,
    <|variantValue: Just(Variant('hello', '1', $tupleVariantType))|>,
];

SELECT
    variantValue.0 AS intAlternative,
    variantValue.1 AS strAlternative
FROM
    as_table($optionalData)
;
