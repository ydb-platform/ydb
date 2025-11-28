USE plato;

$values = ListMap(
    ListFromRange(0, 30),
    ($x) -> (AsStruct($x as x))
);

INSERT INTO @table SELECT * FROM AS_TABLE($values) ORDER BY x;

COMMIT;

$splitter = ($row) -> {
    $recordType = TypeOf($row);
    $varType = VariantType(TupleType($recordType, $recordType, $recordType, $recordType));
    RETURN CASE $row.x
        WHEN 0 THEN Variant($row, "0", $varType)
        WHEN 1 THEN Variant($row, "1", $varType)
        WHEN 2 THEN Variant($row, "2", $varType)
        ELSE        Variant($row, "3", $varType)
    END
};

$a, $b, $c, $d = (PROCESS @table USING $splitter(TableRow()));

SELECT * FROM $a;
SELECT * FROM $b;
SELECT * FROM $c ORDER BY x;
SELECT * FROM $d ORDER BY x;