SELECT
    ListMap(AsList(1,2),($x)->{
        return ListExtend(
            ListMap(AsList(100,101),($y)->{return $x+$y}),
            ListMap(AsList(100,101),($y)->{return $x+$y}))
    })
