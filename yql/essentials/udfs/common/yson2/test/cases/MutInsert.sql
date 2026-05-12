SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m = Yson::MutInsert($m, '1'y);
        return Yson::MutFreeze($m);
    }),
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        $m = Yson::MutInsert($m, '1'y);
        $m = Yson::MutInsert($m, '2'y);
        return Yson::MutFreeze($m);
    });
