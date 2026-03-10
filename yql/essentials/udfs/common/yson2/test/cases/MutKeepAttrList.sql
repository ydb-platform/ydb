SELECT Block(($arg)->{
   $m = Yson::Mutate('<a=1>[2]'y);
   $m = Yson::MutDown($m, '=0');
   $m = Yson::MutUpsert($m, '3'y);
   return Yson::MutFreeze($m);
});
