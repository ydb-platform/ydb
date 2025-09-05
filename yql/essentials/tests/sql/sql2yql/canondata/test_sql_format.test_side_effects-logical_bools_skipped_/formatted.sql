PRAGMA warning('disable', '4510');

SELECT
    (NOT Yql::WithSideEffectsMode(x, AsAtom('General'))) AND TRUE
FROM (
    SELECT
        Ensure(FALSE, TRUE) AS x
);
