$tupleVariantType = Variant<Int32, Int32, Int32>;

$data = [
    <|index: Just(0u)|>,
    <|index: Just(1u)|>,
    <|index: Just(2u)|>,
    <|index: Just(99u)|>,
];

SELECT
    DynamicVariant(42, index, $tupleVariantType) AS variant
FROM
    as_table($data)
;
