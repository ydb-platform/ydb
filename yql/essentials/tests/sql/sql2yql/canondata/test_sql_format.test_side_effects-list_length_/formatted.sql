/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    ListLength([Yql::WithSideEffectsMode(Ensure(1, FALSE), AsAtom('General'))])
;
