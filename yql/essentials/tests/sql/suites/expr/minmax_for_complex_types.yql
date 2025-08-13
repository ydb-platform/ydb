select
    max_of(AsTuple(1u, 2), AsTuple(1, 1/0), AsTuple(1, 3)) as max_tuple,
    min_of(AsTuple(1u, 2), AsTuple(1, 1/0), AsTuple(1, 3)) as min_tuple,

    min_of(AsTuple(0, 1/0), AsTuple(1, 1/0), AsTuple(2, 1/0)) as min_tuple1,
    max_of(AsTuple(0, 1/0), AsTuple(1, 1/0), AsTuple(2, 1/0)) as max_tuple1,

    min_of(AsTuple(1, 1/0), AsTuple(1, 1)) as min_tuple2,
    max_of(AsTuple(1, 1/0), AsTuple(1, 1)) as max_tuple2,
    
    min_of(AsTuple(1, 1/0), AsTuple(1, 1/0)) as min_tuple3,
    max_of(AsTuple(1, 1/0), AsTuple(1, 1/0)) as max_tuple3,
    
    min_of(AsTuple(1, 1/0)) as min_tuple4,
    max_of(AsTuple(1, 1/0)) as max_tuple4,


    min_of(AsList(1, 2, 3), AsList(1, 1)) as min_list,
    max_of(AsList(1, 2, 3), AsList(1, 1)) as max_list,
