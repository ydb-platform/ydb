USE plato;

$values = ListMap(
    ListFromRange(0, 30),
    ($x) -> (AsStruct($x AS x))
);

INSERT INTO @table
SELECT
    *
FROM
    AS_TABLE($values)
ORDER BY
    x DESC
;

COMMIT;

$splitter = ($rows) -> {
    $recordType = StreamItemType(TypeOf($rows));
    $varType = VariantType(TupleType($recordType, $recordType, $recordType, $recordType));
    RETURN Yql::OrderedMap(
        $rows, ($row) -> {
            RETURN CASE $row.x
                WHEN 0 THEN VARIANT ($row, "0", $varType)
                WHEN 1 THEN VARIANT ($row, "1", $varType)
                WHEN 2 THEN VARIANT ($row, "2", $varType)
                ELSE VARIANT ($row, "3", $varType)
            END;
        }
    );
};

$a, $b, $c, $d = (
    PROCESS @table
    USING $splitter(TableRows())
);

SELECT
    *
FROM
    $a
;

SELECT
    *
FROM
    $b
;

SELECT
    *
FROM
    $c
ORDER BY
    x DESC
;

SELECT
    *
FROM
    $d
ORDER BY
    x DESC
;
