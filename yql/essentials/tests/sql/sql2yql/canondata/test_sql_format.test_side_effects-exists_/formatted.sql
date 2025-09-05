/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    just(Yql::WithSideEffectsMode(Ensure(1, FALSE), AsAtom('General'))) IS NOT NULL
;
