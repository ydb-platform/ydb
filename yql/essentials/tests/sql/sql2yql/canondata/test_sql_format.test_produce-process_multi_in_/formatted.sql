/* syntax version 1 */
/* postgres can not */
/* dq file can not */
$udfScript = @@
def MyFunc(stream):
    return stream
@@;

$record = (
    SELECT
        TableRow()
    FROM
        plato.Input
);

$recordType = TypeOf(Unwrap($record));
$streamType = StreamType(VariantType(TupleType($recordType, $recordType, $recordType)));
$udf = Python3::MyFunc(CallableType(0, $streamType, $streamType), $udfScript);

$src = (
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key > '200'
);

$i, $j, $k = (
    PROCESS plato.Input, (
        SELECT
            *
        FROM
            plato.Input
        WHERE
            key > '100'
    ), $src
    USING $udf(TableRows())
);

SELECT
    *
FROM
    $i
;

SELECT
    *
FROM
    $j
;

SELECT
    *
FROM
    $k
;
