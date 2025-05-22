/* syntax version 1 */
$regs = AsList("^a","^b");

$input = AsList("e","aa","et","cb","ba");

$table_input = (select * from (select $input as x) flatten by x);

$compiled_regs = ListMap($regs, ($r)->{
    return Re2::Grep($r);
});

$f = ($s) -> {
    $apply_list = ListMap($compiled_regs, ($cr)->{
        return $cr($s);
    });
   
    $filtered = ListFilter($apply_list, ($m)->{
        return $m;
    });
    
    return ListLength(ListTake($filtered,1)) > 0;
};

select x, $f(x) from $table_input;
