$input = (
    SELECT
        1 AS key,
        'foo' AS value
);

$f = ($row) -> {
    RETURN <|key: $row.key + 1, value: $row.value|>;
};

PROCESS $input
USING $f(TableRow());
