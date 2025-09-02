PRAGMA warning('disable', '4510');

$input = (
    SELECT
        1 AS key,
        'foo' AS value
);

$f = ($rows) -> {
    RETURN Yql::Map($rows, ($row) -> (<|key: $row.key + 1, value: $row.value|>));
};

PROCESS $input
USING $f(TableRows());
