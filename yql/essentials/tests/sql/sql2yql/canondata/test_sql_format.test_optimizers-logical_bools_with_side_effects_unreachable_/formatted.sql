PRAGMA warning('disable', '4510');

SELECT
    FALSE AND (NOT Yql::WithSideEffectsMode(x, AsAtom('General')))
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
