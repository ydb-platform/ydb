SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m = Yson::MutDownOrCreate($m, '>last');
        $m = Yson::MutUpsert($m, '1'y);
        $m = Yson::MutUp($m);
        $m = Yson::MutDownOrCreate($m, '<0');
        $m = Yson::MutUpsert($m, '2u'y);
        $m = Yson::MutRewind($m);
        $m = Yson::MutDown($m, '=0');
        $m = Yson::MutInsert($m, '3.0'y);
        return Yson::MutFreeze($m);
    })
