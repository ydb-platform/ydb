/* custom error: Condition violated */
pragma warning("disable","4510");
select (not Yql::WithSideEffectsMode(x, AsAtom('General'))) and false from (select Ensure(false, false) as x)
