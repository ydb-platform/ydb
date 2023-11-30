/* syntax version 1 */
/* postgres can not */
use plato;

$force_remove_members = ($struct, $to_remove) -> {
    $remover = EvaluateCode(LambdaCode(($st) -> {
        $to_keep = ListFlatMap(StructTypeComponents(TypeHandle(TypeOf($struct))), ($x) -> {return IF($x.Name not in $to_remove, $x.Name)});
        return FuncCode(
            "AsStruct",
            ListMap(
                $to_keep,
                ($x) -> {return ListCode(AtomCode($x), FuncCode("Member", $st, AtomCode($x)))}
            )
        )
    }));
    return $remover($struct)
};

define action $func($input, $output) as
    $jname = $output;
    insert into @$jname
    with truncate
        select
            *
        from $input as input;
    commit;
    insert into $output
        with truncate
    select
        AGG_LIST(
            $force_remove_members(
                TableRow(),
                ['']
            )
        )
    from @$jname;
    commit;
end define;

$exps = [('Input','Output1'),('Input','Output2'),('Input','Output3')];
evaluate for $exp_name in $exps do begin
    $input = $exp_name.0;
    $output = $exp_name.1;
    do $func($input, $output);
end do;
