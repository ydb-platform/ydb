SELECT
    AsList(
        ListMap(AsList(3,4),($x)->{
            return $x+Yql::Fold(ListMap(AsList(5,6),($x)->{
              return $x+1
            }), 0, ($a,$b)->{return $a+$b})
        }),
        ListMap(AsList(1,2),($x)->{
            return $x+1
        })
    );
