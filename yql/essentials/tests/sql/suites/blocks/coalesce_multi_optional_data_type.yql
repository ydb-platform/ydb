$data = [<|optionalValue:  Just(Just(4)), optionalValue2: Just(404)|>,
         <|optionalValue:  Just(Nothing(int32?)), optionalValue2: Just(404)|>,
         <|optionalValue: Just(Just(404)), optionalValue2: Just(404)|>,
         <|optionalValue: Nothing(int32??), optionalValue2: Nothing(int32?)|>,];


SELECT Coalesce(optionalValue, optionalValue2) FROM as_table($data);
