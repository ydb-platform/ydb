SELECT
    AsList(
        ListMap(AsList(33,42),($x)->{
            return MAX_OF($x,
            Yql::Fold(ListMap(AsList(7,8),($y)->{
              return MAX_OF($x,Yql::Fold(AsList(11,12), 2, ($e,$f)->{return MAX_OF($e,$f,$y)}))
            }), 0, ($a,$b)->{return MAX_OF($a,$b,Yql::Fold(AsList(13,14), 3, ($i,$j)->{return MAX_OF($i,$j,$a)}))}),
            Yql::Fold(AsList(9,10), 1, ($c,$d)->{return MAX_OF($c,$d,Yql::Fold(ListMap(AsList(5,6),($y)->{
              return MAX_OF($x,Yql::Fold(AsList(11,12), 2, ($e,$f)->{return MAX_OF($f,$y,$e)}))
            }), 0, ($a,$b)->{return MAX_OF(Yql::Fold(AsList(13,14), 3, ($i,$j)->{return MAX_OF($j,$a,$i)}),$a,$b)}))}))
        })
    );

