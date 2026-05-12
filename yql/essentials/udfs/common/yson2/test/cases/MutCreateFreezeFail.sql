SELECT 
    Block(($x)->{
        $m = Udf(Yson::MutCreate, $x as Depends)();
        return Yson::MutFreeze($m);
    });
    