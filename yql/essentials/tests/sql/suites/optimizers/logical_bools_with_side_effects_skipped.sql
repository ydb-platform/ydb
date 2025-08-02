pragma warning("disable","4510");
select (not Yql::WithSideEffectsMode(x, AsAtom('General'))) and true from (select Ensure(false, true) as x)
