$data = [
    <|optionalValue: Just(4p), missingValue: -1p|>,
    <|optionalValue: Nothing(pgint4?), missingValue: -2p|>,
    <|optionalValue: Just(3p), missingValue: -3p|>,
    <|optionalValue: Nothing(pgint4?), missingValue: -4p|>,
];

$f = ($x) -> {
    RETURN $x * 2p;
};

SELECT
    YQL::IfPresent(optionalValue, $f, missingValue)
FROM
    as_table($data)
;
