USE plato;
/* syntax version 1 */
/* postgres can not */
$udfScript = @@
def MyFunc(list):
    return [(int(x.key) % 4, x) for x in list]
@@;

$record = (
    SELECT
        TableRow()
    FROM
        Input
);
$recordType = TypeOf(Unwrap($record));
$udf = Python::MyFunc(
    CallableType(
        0,
        StreamType(
            VariantType(TupleType($recordType, $recordType, $recordType, $recordType))
        ),
        StreamType($recordType)
    ),
    $udfScript
);

$i0, $i1, $i2, $i3 = (
    PROCESS Input
    USING $udf(TableRows())
);

SELECT
    *
FROM (
    SELECT
        *
    FROM
        $i0
    UNION ALL
    SELECT
        *
    FROM
        $i1
    UNION ALL
    SELECT
        *
    FROM
        $i2
    UNION ALL
    SELECT
        *
    FROM
        $i3
)
ORDER BY
    key
;

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        *
    FROM
        $i0
    UNION ALL
    SELECT
        *
    FROM
        $i1
    UNION ALL
    SELECT
        *
    FROM
        $i2
    UNION ALL
    SELECT
        *
    FROM
        $i3
);
