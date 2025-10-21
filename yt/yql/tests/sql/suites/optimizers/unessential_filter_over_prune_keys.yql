use plato;

$id = ($x) -> {
    return $x;
};

$prunekeys = ($world, $recs) -> {
    return YQL::PruneKeys($recs($world), $id);
};

define subquery $input() as
    select * from Input;
end define;

select * from $prunekeys($input) where YQL::Unessential(value != "not present", true);
