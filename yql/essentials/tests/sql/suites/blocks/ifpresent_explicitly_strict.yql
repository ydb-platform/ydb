$data = [<|optionalValue: Just(4)|>,
         <|optionalValue: Nothing(int32?)|>,
         <|optionalValue: Just(404)|>,
         <|optionalValue: Nothing(int32?)|>,];

$f = ($x) -> {
    return AssumeStrict(AssumeNonStrict($x * 2));
};

SELECT YQL::IfPresent(optionalValue, $f, -5) FROM as_table($data);
