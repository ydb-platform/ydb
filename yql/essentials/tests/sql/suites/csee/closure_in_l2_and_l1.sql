SELECT
    AsList(
        ListMap(AsList(3,4),($x)->{
            return $x
            +Yql::Fold(ListMap(AsList(7,8),($y)->{
              return $x+$y
            }), 0, ($a,$b)->{return $a+$b})
            +Yql::Fold(AsList(9,10), 1, ($c,$d)->{return $c + $d + Yql::Fold(ListMap(AsList(5,6),($y)->{
              return $x+$y
            }), 0, ($a,$b)->{return $a+$b})})
        })
    );
