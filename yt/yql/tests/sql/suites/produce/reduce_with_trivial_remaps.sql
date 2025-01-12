use plato;
pragma warning("disable", "4510");

$udf = ($_key, $stream) -> {
   $init = ($item) -> (AsStruct(1u as cnt, $item as row));
   $switch = ($_item, $_state) -> (false);
   $update = ($item, $state) -> (AsStruct($state.cnt + 1u as cnt,
      if(($item.value > $state.row.value) ?? false, $item, $state.row) as row));
   $state = YQL::Collect(YQL::Condense1($stream, $init, $switch, $update));
   return $state;
};

REDUCE CONCAT(Input1,Input2)
presort subkey
ON key USING $udf(TableRow());
