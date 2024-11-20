/* postgres can not */
/* syntax version 1 */
$a = AsList(1,2,3,1,2,3);

select
    ListTakeWhile($a,($x)->{return $x<3}),
    ListSkipWhile($a,($x)->{return $x<3}),
    Yql::Collect(YQL::TakeWhile(Yql::Iterator($a,Yql::DependsOn(1)),($x)->{return $x<3})),
    Yql::Collect(YQL::SkipWhile(Yql::Iterator($a,Yql::DependsOn(2)),($x)->{return $x<3})),
    Yql::TakeWhile(Just(1),($x)->{return $x<3}),
    Yql::SkipWhile(Just(1),($x)->{return $x<3});
