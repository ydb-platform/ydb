PRAGMA config.flags("NormalizeDependsOn");

$input = ListMap(ListFromRange(1ul, 11ul), ($i) -> (AsStruct($i AS key)));

SELECT
    key,
    row_number() OVER (
        ORDER BY random(TableRow())
    ) AS must_be_shuffled
FROM AS_TABLE($input)
ORDER BY key;
