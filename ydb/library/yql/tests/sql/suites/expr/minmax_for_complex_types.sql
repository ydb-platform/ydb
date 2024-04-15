select
    max_of(AsTuple(1u, 2), AsTuple(1, 1/0), AsTuple(1, 3)) as max_tuple,
    min_of(AsList(1, 2, 3), AsList(1, 1)) as min_list,
;
