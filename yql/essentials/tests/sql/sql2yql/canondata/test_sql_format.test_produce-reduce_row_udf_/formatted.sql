PRAGMA warning('disable', '4510');

$input = (
    SELECT
        1 AS key,
        'foo' AS value
);

$r = (
    REDUCE $input
    ON
        key
    USING SimpleUdf::GenericAsStruct(TableRow().value)
);

SELECT
    arg_0,
    Yql::Collect(arg_1)
FROM
    $r
;
