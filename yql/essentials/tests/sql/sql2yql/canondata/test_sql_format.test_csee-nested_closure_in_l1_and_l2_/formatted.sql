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
                                        RETURN $x + Yql::Fold(
                                            AsList(11, 12), 2, ($e, $f) -> {
                                                RETURN $e + $f + $y;
                                            }
                                        );
                                    }
                                ), 0, ($a, $b) -> {
                                    RETURN $a + $b + Yql::Fold(
                                        AsList(13, 14), 3, ($i, $j) -> {
                                            RETURN $i + $j + $a;
                                        }
                                    );
                                }
                            );
                        }
                    )
                    + Yql::Fold(
                        ListMap(
                            AsList(7, 8), ($y) -> {
                                RETURN $x + Yql::Fold(
                                    AsList(11, 12), 2, ($e, $f) -> {
                                        RETURN $e + $f + $y;
                                    }
                                );
                            }
                        ), 0, ($a, $b) -> {
                            RETURN $a + $b + Yql::Fold(
                                AsList(13, 14), 3, ($i, $j) -> {
                                    RETURN $i + $j + $a;
                                }
                            );
                        }
                    );
            }
        )
    )
;
