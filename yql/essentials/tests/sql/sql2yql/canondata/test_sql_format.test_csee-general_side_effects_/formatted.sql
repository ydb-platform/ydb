PRAGMA warning('disable', '4510');

SELECT
    Yql::WithSideEffectsMode(1, AsAtom('General'))
    + Yql::WithSideEffectsMode(1, AsAtom('General'))
;
