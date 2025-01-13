/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $t() AS
    SELECT
        *
    FROM
        as_table([<|key: '0'|>, <|key: '1'|>])
    ;
END DEFINE;

DEFINE SUBQUERY $split_formula_log($in) AS
    $parition = ($row) -> {
        $recordType = TypeOf($row);
        $varType = VariantType(
            TupleType(
                $recordType,
                $recordType
            )
        );
        RETURN CASE
            WHEN $row.key == '0' THEN VARIANT ($row, '0', $varType)
            WHEN $row.key == '1' THEN VARIANT ($row, '1', $varType)
            ELSE NULL
        END;
    };

    PROCESS $in()
    USING $parition(TableRow());
END DEFINE;

$a, $b = (
    PROCESS $split_formula_log($t)
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
