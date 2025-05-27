/* syntax version 1 */
/* postgres can not */
select EvaluateCode(FuncCode("Int32",AtomCode("1")));

$inc = EvaluateCode(LambdaCode(($x)->{return 
    FuncCode("+", $x, FuncCode("Int32", AtomCode("1")))}));
select $inc(1);

$addPrefixForMembers = ($strValue)->{
    $code = EvaluateCode(LambdaCode(($str)->{
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
        $list = ListMap($members, ($x)->{
            return ListCode(AtomCode("prefix" || $x.Name),FuncCode("Member", $str, AtomCode($x.Name)));
        });

        return FuncCode("AsStruct",$list);
    }));
    return $code($strValue);
};

select $addPrefixForMembers(AsStruct(1 as foo, "2" as bar));

