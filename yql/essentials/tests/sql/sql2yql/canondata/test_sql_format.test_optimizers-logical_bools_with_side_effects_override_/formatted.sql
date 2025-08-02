PRAGMA warning('disable', '4510');

SELECT
    Yql::WithSideEffectsMode((NOT Yql::WithSideEffectsMode(x, AsAtom('General'))), AsAtom('None'))
    AND FALSE
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
