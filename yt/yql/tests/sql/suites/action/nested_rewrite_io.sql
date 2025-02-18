use plato;
$input = select * from AS_TABLE([<|a:"foo",b:"123"|>]);

$mapping = select {"a":"String", "b":"Int32"} from Input limit 1;

$transformer = ($type)->{
    $t = EvaluateType(ParseTypeHandle($type));
    return ($value)->{ return cast($value as $t); };
};

$converter = ($row)->{
   return EvaluateCode(LambdaCode(($rowCode)->{
       return FuncCode("AsStruct", ListMap(StructMembers($row), ($name)->{
           return ListCode(
               AtomCode($name),
               FuncCode("Apply", QuoteCode($transformer(Unwrap($mapping[$name]))), FuncCode("Member", $rowCode, AtomCode($name)))
           );
       }));
   }))($row);
};

select * from (select $converter(TableRow()) from $input) flatten columns;
