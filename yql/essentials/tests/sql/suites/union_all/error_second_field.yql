pragma warning("disable","4510");
select y from (
select 0 as y, 1 as x
union all 
select 1 as y, Yql::Error(Yql::ErrorType(AsAtom('1'),AsAtom('2'),AsAtom(''),AsAtom('foo'))) as x
)

