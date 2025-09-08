/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    if(TRUE, Yql::WithSideEffectsMode(ensure(NULL, FALSE), AsAtom('General')), 1)
;
