$structVariantType = Variant<x: Int32?, y: Int32?>;

$data = [
    <|payload: Just(10), index: Just("x"u)|>,
    <|payload: Nothing(Int32?), index: Just("y"u)|>,
    <|payload: Just(30), index: Just("z"u)|>,
    <|payload: Just(40), index: Nothing(Utf8?)|>,
];

SELECT
    DynamicVariant(payload, index, $structVariantType) AS variant
FROM
    as_table($data)
;
