SELECT
    AsList(
        ListMap(
            AsList(3, 4), ($x) -> {
                RETURN $x
                    + Yql::Fold(
                        AsList(9, 10), 1, ($c, $d) -> {
                            RETURN $c + $d + Yql::Fold(
                                ListMap(
                                    AsList(5, 6), ($y) -> {
                                        RETURN $x + $y;
                                    }
                                ), 0, ($a, $b) -> {
                                    RETURN $a + $b;
                                }
                            );
                        }
                    )
                    + Yql::Fold(
                        ListMap(
                            AsList(7, 8), ($y) -> {
                                RETURN $x + $y;
                            }
                        ), 0, ($a, $b) -> {
                            RETURN $a + $b;
                        }
                    );
            }
        )
    )
;
