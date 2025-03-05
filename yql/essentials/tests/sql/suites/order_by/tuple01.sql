/* postgres can not */

select Yql::Sort(
AsList(3,2,1),
AsTuple(),
($_x)->{return AsTuple()}
);


select Yql::Sort(
AsList(3,2,1),
AsTuple(true),
($x)->{return $x}
);

select Yql::Sort(
AsList(3,2,1),
true,
($x)->{return $x}
);

select Yql::Sort(
AsList(3,2,1),
AsTuple(true),
($x)->{return AsTuple($x)}
);

select Yql::Sort(
AsList(3,2,1),
AsTuple(true,true),
($x)->{return AsTuple($x,$x)}
);
