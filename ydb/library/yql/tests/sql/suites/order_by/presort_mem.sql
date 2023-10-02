/* postgres can not */
select Yql::Sort(
    AsList(
        AsTuple(3,1),
        AsTuple(1,2),
        AsTuple(1,3),
    ),
    true,
    ($x)->{return $x;}
);

select Yql::Sort(
    AsList(
        AsTuple(3,1),
        AsTuple(1,1),
        AsTuple(1,3),
    ),
    AsTuple(true, false),
    ($x)->{return $x;}
);

select Yql::Sort(
    AsList(
        AsList(3,1),
        AsList(1,2),
        AsList(2,3),
        AsList(1,2,3)
    ),
    true,
    ($x)->{return $x;}
);

select Yql::Sort(
    AsList(
        AsTuple(1, AsList(3,1)),
        AsTuple(1, AsList(1,2)),
        AsTuple(1, AsList(2,3)),
        AsTuple(1, AsList(1,2,3)),
        AsTuple(2, AsList(3,1)),
        AsTuple(2, AsList(1,2)),
        AsTuple(2, AsList(2,3)),
        AsTuple(2, AsList(1,2,3))
    ),
    AsTuple(true, false),
    ($x)->{return $x;}
);

select Yql::Sort(
    AsList(
        AsTuple(1, AsList(3,1)),
        AsTuple(1, AsList(1,2)),
        AsTuple(1, AsList(2,3)),
        AsTuple(1, AsList(1,2,3)),
        AsTuple(2, AsList(3,1)),
        AsTuple(2, AsList(1,2)),
        AsTuple(2, AsList(2,3)),
        AsTuple(2, AsList(1,2,3))
    ),
    AsTuple(true, true),
    ($x)->{return $x;}
);

select Yql::Sort(
    AsList(
        AsTuple(1, AsList(3,1)),
        AsTuple(1, AsList(1,2)),
        AsTuple(1, AsList(2,3)),
        AsTuple(1, AsList(1,2,3)),
        AsTuple(2, AsList(3,1)),
        AsTuple(2, AsList(1,2)),
        AsTuple(2, AsList(2,3)),
        AsTuple(2, AsList(1,2,3))
    ),
    false,
    ($x)->{return $x;}
);
