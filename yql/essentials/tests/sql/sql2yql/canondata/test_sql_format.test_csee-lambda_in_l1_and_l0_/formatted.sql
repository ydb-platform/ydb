SELECT
    AsList(
        ListMap(
            AsList(3, 4), ($x) -> {
                RETURN $x + Yql::Fold(
                    ListMap(
                        AsList(5, 6), ($x) -> {
                            RETURN $x + 1;
                        }
                    ), 0, ($a, $b) -> {
                        RETURN $a + $b;
                    }
                );
            }
        ),
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + 1;
            }
        )
    )
;
