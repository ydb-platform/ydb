SELECT Block(($arg)->{
   $m = Yson::Mutate('<a=1>{b=2}'y);
   $m = Yson::MutDown($m, 'b');
   $m = Yson::MutUpsert($m, '3'y);
   return Yson::MutFreeze($m);
});
