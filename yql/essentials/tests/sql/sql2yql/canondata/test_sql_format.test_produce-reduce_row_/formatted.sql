PRAGMA warning('disable', '4510');

$input = (
    SELECT
        1 AS key,
        'foo' AS value
);

$f = ($key, $stream) -> {
    RETURN <|key: $key, len: ListLength(Yql::Collect($stream))|>;
};

REDUCE $input
ON
    key
USING $f(TableRow());
