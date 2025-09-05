/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    ListExtend([1], Yql::WithSideEffectsMode(Ensure(ListTake([2], 0), FALSE), AsAtom('General')))
;
