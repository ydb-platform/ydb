SELECT
    AsList(
        ListMap(AsList(1,2),($x)->{
            return ($x+1)*($x+1)+Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b})
        }),
        ListMap(AsList(10,11),($x)->{
            return ($x+1)*($x+1)+Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b})
        })
    );