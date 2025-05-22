SELECT
    AsTuple(
            YQL::FlatMap(3/1, ($x)->{
                return YQL::FlatMap(1/1,($y)->{
            return Just($x + $y)
            })}),
            YQL::FlatMap(4/1, ($x)->{
                return YQL::FlatMap(1/1,($y)->{
            return Just($x + $y)
            })})
        )
