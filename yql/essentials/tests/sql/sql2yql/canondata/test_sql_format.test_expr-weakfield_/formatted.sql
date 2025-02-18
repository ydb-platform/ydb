SELECT
    WeakField(a, Int32),
    WeakField(b, String)
FROM (
    SELECT
        {'a': '1'} AS _other
);
