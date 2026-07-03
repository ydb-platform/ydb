$variantWithOptionalMemberType = Variant<a: Optional<Int32>, b: String>;

$optionalVariantWithOptionalMemberData = [
    <|variantValue: Just(Variant(Just(10), 'a', $variantWithOptionalMemberType))|>,
    <|variantValue: Just(Variant(Nothing(Int32?), 'a', $variantWithOptionalMemberType))|>,
    <|variantValue: Nothing(OptionalType($variantWithOptionalMemberType))|>,
    <|variantValue: Just(Variant('world', 'b', $variantWithOptionalMemberType))|>,
];

SELECT
    variantValue.a AS aAlternative,
    variantValue.b AS bAlternative
FROM
    as_table($optionalVariantWithOptionalMemberData)
;
