/* syntax version 1 */
/* postgres can not */
$func = ($x)->{
    return $x == 1;
};

$structApply = ($strValue, $f)->{
    $code = EvaluateCode(LambdaCode(($strCode)->{
        $members = StructTypeComponents(TypeHandle(TypeOf($strValue)));
       
        return Yql::Fold($members, ReprCode(false), ($item, $state)->{
            $member = FuncCode("Member", $strCode, AtomCode($item.Name));
            $apply = FuncCode("Apply", QuoteCode($f), $member);
            return FuncCode("Or", $state, $apply);
        });
    }));
    return $code($strValue);
};

select $structApply(AsStruct(1 as a,2 as b,3 as c), $func);
select $structApply(AsStruct(4 as a,2 as b,3 as c), $func);
