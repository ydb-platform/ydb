SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m, $v1 = Yson::MutView($m);
        $m = Yson::MutUpsert($m, 'foo'y);
        $m, $v2 = Yson::MutView($m);
        return LinearDestroy([$v1, $v2], $m);
    });
