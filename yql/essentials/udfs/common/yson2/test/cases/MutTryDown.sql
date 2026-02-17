SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m, $ok = Yson::MutTryDown($m, 'foo');
        $m = Ensure($m, not $ok);
        $m = Yson::MutDownOrCreate($m, 'foo');
        $m = Yson::MutUpsert($m, '1'y);
        $m = Yson::MutRewind($m);
        $m, $ok = Yson::MutTryDown($m, 'foo');
        return LinearDestroy($ok, $m);
    })
