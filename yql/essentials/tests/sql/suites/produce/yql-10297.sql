/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $t() AS
  select * from as_table([<|key:"0"|>, <|key:"1"|>]);
END DEFINE;

DEFINE SUBQUERY $split_formula_log($in) AS
    $parition = ($row) -> {
        $recordType = TypeOf($row);
        $varType = VariantType(TupleType($recordType, 
                                        $recordType));
        RETURN case
                when $row.key = "0" then
                Variant($row, "0", $varType)
                
                when $row.key = "1" then
                Variant($row, "1", $varType)
                else null
            end
        ;
    };
    
    PROCESS $in() USING $parition(TableRow());
END DEFINE;


$a, $b = (PROCESS $split_formula_log($t));
select * from $a;
select * from $b;

