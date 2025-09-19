SELECT
    AsList(
        ListMap(AsList(3,4),($x)->{
            return $x
            +Yql::Fold(ListMap(AsList(7,8),($y)->{
              return $x+Yql::Fold(AsList(11,12), 2, ($e,$f)->{return $e+$f+$y})
            }), 0, ($a,$b)->{return $a+$b+Yql::Fold(AsList(13,14), 3, ($i,$j)->{return $i+$j+$a})})
            +Yql::Fold(AsList(9,10), 1, ($c,$d)->{return $c + $d + Yql::Fold(ListMap(AsList(5,6),($y)->{
              return $x+Yql::Fold(AsList(11,12), 2, ($e,$f)->{return $e+$f+$y})
            }), 0, ($a,$b)->{return $a+$b+Yql::Fold(AsList(13,14), 3, ($i,$j)->{return $i+$j+$a})})})
        })
    );
