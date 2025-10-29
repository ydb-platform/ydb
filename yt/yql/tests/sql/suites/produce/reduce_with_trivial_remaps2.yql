use plato;
pragma EmitUnionMerge;


use plato;
pragma warning("disable", "4510");

$process_stream = ($stream) -> {
   $init = ($item) -> (AsStruct(1u as cnt, $item as row));
   $switch = ($_item, $_state) -> (false);
   $update = ($item, $state) -> (AsStruct($state.cnt + 1u as cnt,
       if(($item.value > $state.row.value) ?? false, $item, $state.row) as row));
   $state = YQL::Collect(YQL::Condense1($stream, $init, $switch, $update));
   return $state;
};

$udf = ($_key, $variant_stream) -> {
   $itentity = ($x)->($x);
   return $process_stream(YQL::OrderedMap($variant_stream, ($item)->(YQL::Visit($item, AsAtom('0'), $itentity, AsAtom('1'), $itentity))));
};

$src1 = select * from Input1 assume order by key, subkey;
$src2 = select key, subkey, cast(value as String) as value from Input2 assume order by key, subkey;
$src3 = select * from Input3 assume order by key, subkey;

$src = select * from $src1 union all select * from $src2 union all select * from $src3;

REDUCE $src, $src1 presort subkey ON key USING $udf(TableRow());

