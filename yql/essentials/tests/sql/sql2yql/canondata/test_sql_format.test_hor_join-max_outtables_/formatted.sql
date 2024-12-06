/* syntax version 1 */
/* postgres can not */
/* kikimr can not */
PRAGMA yt.MaxOutputTables = "3";

$i1 = (
    SELECT
        CAST(key AS Int32) ?? 0 AS key,
        '' AS value
    FROM
        plato.Input1
);

$i2 = (
    SELECT
        0 AS key,
        value
    FROM
        plato.Input2
);

$i3 = (
    SELECT
        1 AS key,
        value
    FROM
        plato.Input3
);

$i4 = (
    SELECT
        2 AS key,
        value
    FROM
        plato.Input4
);

$i5 = (
    SELECT
        3 AS key,
        value
    FROM
        plato.Input5
);
$udf = ($x) -> {
    RETURN Yql::VariantItem($x)
};

PROCESS $i1, $i2, $i3, $i4, $i5
USING $udf(TableRow());
