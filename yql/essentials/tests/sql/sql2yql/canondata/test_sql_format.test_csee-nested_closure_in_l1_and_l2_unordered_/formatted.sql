SELECT
    AsList(
        ListMap(
            AsList(3, 4), ($x) -> {
                RETURN MIN_OF(
                    $x,
                    Yql::Fold(
                        AsList(9, 10), 100, ($c, $d) -> {
                            RETURN MIN_OF(
                                $c, $d, Yql::Fold(
                                    ListMap(
                                        AsList(5, 6), ($y) -> {
                                            RETURN MIN_OF(
                                                $x, Yql::Fold(
                                                    AsList(11, 12), 10, ($e, $f) -> {
                                                        RETURN MIN_OF($f, $y, $e);
                                                    }
                                                )
                                            );
                                        }
                                    ), 100, ($a, $b) -> {
                                        RETURN MIN_OF(
                                            $a, $b, Yql::Fold(
                                                AsList(13, 14), 100, ($i, $j) -> {
                                                    RETURN MIN_OF($i, $j, $a);
                                                }
                                            )
                                        );
                                    }
                                )
                            );
                        }
                    ),
                    Yql::Fold(
                        ListMap(
                            AsList(7, 8), ($y) -> {
                                RETURN MIN_OF(
                                    $x, Yql::Fold(
                                        AsList(11, 12), 10, ($e, $f) -> {
                                            RETURN MIN_OF($e, $f, $y);
                                        }
                                    )
                                );
                            }
                        ), 100, ($a, $b) -> {
                            RETURN MIN_OF(
                                $b, Yql::Fold(
                                    AsList(13, 14), 100, ($i, $j) -> {
                                        RETURN MIN_OF($a, $j, $i);
                                    }
                                ), $a
                            );
                        }
                    )
                );
            }
        )
    )
;
