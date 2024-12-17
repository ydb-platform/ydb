/* syntax version 1 */
/* postgres can not */
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
    UNION ALL
    SELECT
        1 AS key,
        value
    FROM
        plato.Input3
);

$i3 = (
    SELECT
        2 AS key,
        value
    FROM
        plato.Input4
    UNION ALL
    SELECT
        3 AS key,
        value
    FROM
        plato.Input5
);

$udf = ($x) -> {
    RETURN Yql::VariantItem($x);
};

PROCESS $i1, $i2, $i3
USING $udf(TableRow());
