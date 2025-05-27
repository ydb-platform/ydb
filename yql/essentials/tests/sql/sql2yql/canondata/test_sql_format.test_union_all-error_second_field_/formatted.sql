PRAGMA warning('disable', '4510');

SELECT
    y
FROM (
    SELECT
        0 AS y,
        1 AS x
    UNION ALL
    SELECT
        1 AS y,
        Yql::Error(Yql::ErrorType(AsAtom('1'), AsAtom('2'), AsAtom(''), AsAtom('foo'))) AS x
);
