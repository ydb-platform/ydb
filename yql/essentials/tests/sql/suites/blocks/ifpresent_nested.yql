$data = [<|optionalValue:  Just(Just(4)), missingValue: -1|>,
         <|optionalValue:  Just(Nothing(int32?)), missingValue: -2|>,
         <|optionalValue: Just(Just(404)), missingValue: -3|>,
         <|optionalValue: Nothing(int32??), missingValue: -4|>,];


$g = ($y) -> {
    return $y * 4;
};

$f = ($x) -> {
    return YQL::IfPresent($x, $g, 55);
};

SELECT YQL::IfPresent(optionalValue, $f, missingValue) FROM as_table($data);
