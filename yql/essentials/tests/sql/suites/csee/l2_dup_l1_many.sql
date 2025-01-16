SELECT
    AsList(
        ListMap(AsList(1,2),($_x)->{
            return Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b}) + Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b})
        }),
        ListMap(AsList(10,11),($_x)->{
            return Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b}) * Yql::Fold(AsList(1),0,($a,$b)->{return $a+$b})
        })
    );
