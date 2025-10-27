$data = [<|optionalValue: Just(4)|>,
         <|optionalValue: Nothing(int32?)|>,
         <|optionalValue: Just(404)|>,
         <|optionalValue: Nothing(int32?)|>,];


$f = ($x) -> {
    return Just($x * 2);
};

SELECT YQL::IfPresent(optionalValue, $f, Null) FROM as_table($data);
