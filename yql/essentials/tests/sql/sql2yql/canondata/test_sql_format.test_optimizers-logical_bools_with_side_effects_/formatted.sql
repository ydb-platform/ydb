/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    (NOT Yql::WithSideEffectsMode(x, AsAtom('General'))) AND FALSE
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
