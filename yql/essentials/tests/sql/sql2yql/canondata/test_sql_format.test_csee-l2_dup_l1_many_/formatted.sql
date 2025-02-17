SELECT
    AsList(
        ListMap(
            AsList(1, 2), ($_x) -> {
                RETURN Yql::Fold(
                    AsList(1), 0, ($a, $b) -> {
                        RETURN $a + $b;
                    }
                ) + Yql::Fold(
                    AsList(1), 0, ($a, $b) -> {
                        RETURN $a + $b;
                    }
                );
            }
        ),
        ListMap(
            AsList(10, 11), ($_x) -> {
                RETURN Yql::Fold(
                    AsList(1), 0, ($a, $b) -> {
                        RETURN $a + $b;
                    }
                ) * Yql::Fold(
                    AsList(1), 0, ($a, $b) -> {
                        RETURN $a + $b;
                    }
                );
            }
        )
    )
;
