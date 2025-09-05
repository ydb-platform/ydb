pragma warning("disable","4510");
select Yql::WithSideEffectsMode((not Yql::WithSideEffectsMode(x, AsAtom('General'))),AsAtom('None'))
    and false from (select Ensure(false, false) as x)
