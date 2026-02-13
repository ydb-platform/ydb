SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m, $e1 = Yson::MutExists($m);
        $m = Yson::MutUpsert($m, 'foo'y);
        $m, $e2 = Yson::MutExists($m);
        return LinearDestroy([$e1, $e2], $m);
    });
