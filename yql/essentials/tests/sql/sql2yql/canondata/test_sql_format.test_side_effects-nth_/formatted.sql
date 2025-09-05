/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    (1, Yql::WithSideEffectsMode(Ensure(2, FALSE), AsAtom('General'))).0
;
