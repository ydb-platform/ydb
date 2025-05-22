PRAGMA warning('disable', '4510');

$input = (
    SELECT
        1 AS key,
        'foo' AS value
);

$f = ($stream) -> {
    RETURN Yql::Map($stream, ($p) -> (<|key: $p.0, len: ListLength(Yql::Collect($p.1))|>));
};

REDUCE $input
ON
    key
USING ALL $f(TableRow());
