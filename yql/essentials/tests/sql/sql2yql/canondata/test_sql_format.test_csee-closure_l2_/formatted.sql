SELECT
    ListMap(
        AsList(1, 2), ($x) -> {
            RETURN ListMap(
                AsList(100, 101), ($y) -> {
                    RETURN $x + $y;
                }
            );
        }
    )
;
