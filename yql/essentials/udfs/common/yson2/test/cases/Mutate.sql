SELECT 
    Block(($x)->{
        $m = Udf(Yson::Mutate, $x as Depends)('1'y);
        return Yson::MutFreeze($m);
    });
