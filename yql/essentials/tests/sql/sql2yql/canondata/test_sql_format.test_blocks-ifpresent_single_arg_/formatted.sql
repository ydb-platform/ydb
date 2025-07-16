$data = [
    <|optionalValue: Just(4), missingValue: -1|>,
    <|optionalValue: Nothing(int32?), missingValue: -2|>,
    <|optionalValue: Just(404), missingValue: -3|>,
    <|optionalValue: Nothing(int32?), missingValue: -4|>,
];

$f = ($x) -> {
    RETURN $x * 2;
};

SELECT
    YQL::IfPresent(optionalValue, $f, missingValue)
FROM
    as_table($data)
;
