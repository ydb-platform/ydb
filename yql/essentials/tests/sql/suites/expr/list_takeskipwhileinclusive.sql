/* postgres can not */
/* syntax version 1 */
$a = AsList(1,2,3,1,2,3);

select
    ListTakeWhileInclusive($a,($x)->{return $x<3}),
    ListSkipWhileInclusive($a,($x)->{return $x<3}),
    Yql::Collect(YQL::TakeWhileInclusive(Yql::Iterator($a,Yql::DependsOn(1)),($x)->{return $x<3})),
    Yql::Collect(YQL::SkipWhileInclusive(Yql::Iterator($a,Yql::DependsOn(2)),($x)->{return $x<3})),
    Yql::TakeWhileInclusive(Just(1),($x)->{return $x<3}),
    Yql::SkipWhileInclusive(Just(1),($x)->{return $x<3});
