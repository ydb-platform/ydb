/* postgres can not */
/* syntax version 1 */
/* ignore runonopt plan diff */
USE plato;

PRAGMA warning('disable', '4510');

$r1 = (
    REDUCE Input0
    ON
        key
    USING ALL SimpleUdf::GenericAsStruct(TableRows())
);

$r2 = (
    REDUCE Input0
    ON
        key
    USING SimpleUdf::GenericAsStruct(CAST(TableRow().subkey AS Int32))
);

$r3 = (
    REDUCE Input0
    ON
        key
    USING ALL SimpleUdf::GenericAsStruct(TableRow().key)
);

SELECT
    *
FROM (
    SELECT
        *
    FROM
        $r1
        FLATTEN LIST BY arg_0
)
    FLATTEN COLUMNS
ORDER BY
    key,
    subkey
;

SELECT
    arg_0 AS key,
    ListSort(YQL::Collect(arg_1)) AS values
FROM
    $r2
ORDER BY
    key
;

SELECT
    FormatType(TypeOf(TableRow()))
FROM
    $r1
LIMIT 1;

SELECT
    FormatType(TypeOf(TableRow()))
FROM
    $r3
LIMIT 1;
