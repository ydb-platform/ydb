/* syntax version 1 */
/* postgres can not */
USE plato;

$myAddSuffix = ($row, $value) -> {
    $type = TypeOf($row);
    --$type=Struct<key:String,subkey:String,value:String>;
    $lambda = EvaluateCode(LambdaCode(($r)->{
        return FuncCode("AsStruct",
            ListMap(StructTypeComponents(TypeHandle($type)),
                ($i)->{ return ListCode(
                    AtomCode($i.Name),
                    FuncCode("Concat",
                        FuncCode("Member",$r,AtomCode($i.Name)),
                        ReprCode($value)
                    )) }));
        
    }));
    
    return $lambda($row);
};

SELECT
    $myAddSuffix(TableRow(), "*")
FROM Input;
