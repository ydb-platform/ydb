SELECT
    AsList(
        ListMap(
            AsList(33, 42), ($x) -> {
                RETURN MAX_OF(
                    $x,
                    Yql::Fold(
                        ListMap(
                            AsList(7, 8), ($y) -> {
                                RETURN MAX_OF(
                                    $x, Yql::Fold(
                                        AsList(11, 12), 2, ($e, $f) -> {
                                            RETURN MAX_OF($e, $f, $y);
                                        }
                                    )
                                );
                            }
                        ), 0, ($a, $b) -> {
                            RETURN MAX_OF(
                                $a, $b, Yql::Fold(
                                    AsList(13, 14), 3, ($i, $j) -> {
                                        RETURN MAX_OF($i, $j, $a);
                                    }
                                )
                            );
                        }
                    ),
                    Yql::Fold(
                        AsList(9, 10), 1, ($c, $d) -> {
                            RETURN MAX_OF(
                                $c, $d, Yql::Fold(
                                    ListMap(
                                        AsList(5, 6), ($y) -> {
                                            RETURN MAX_OF(
                                                $x, Yql::Fold(
                                                    AsList(11, 12), 2, ($e, $f) -> {
                                                        RETURN MAX_OF($f, $y, $e);
                                                    }
                                                )
                                            );
                                        }
                                    ), 0, ($a, $b) -> {
                                        RETURN MAX_OF(
                                            Yql::Fold(
                                                AsList(13, 14), 3, ($i, $j) -> {
                                                    RETURN MAX_OF($j, $a, $i);
                                                }
                                            ), $a, $b
                                        );
                                    }
                                )
                            );
                        }
                    )
                );
            }
        )
    )
;
