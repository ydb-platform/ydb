$data = [<|optionalValue: Just(4), optionalValue2: Just(404), missingValue: -1|>,
         <|optionalValue: Nothing(int32?), optionalValue2: Just(404), missingValue: -2|>,
         <|optionalValue: Just(404), optionalValue2: Just(404), missingValue: -3|>,
         <|optionalValue: Nothing(int32?), optionalValue2: Nothing(int32?), missingValue: -4|>,];

$g = ($x, $g) -> {
    return $x * $g;
};

SELECT YQL::IfPresent(optionalValue, optionalValue2, $g, missingValue), FROM as_table($data);
