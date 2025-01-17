SELECT
    ListMap(
        AsList(1, 2), ($x) -> {
            RETURN ListExtend(
                ListMap(
                    AsList(100, 101), ($y) -> {
                        RETURN $x + $y;
                    }
                ),
                ListMap(
                    AsList(100, 101), ($y) -> {
                        RETURN $x + $y;
                    }
                )
            );
        }
    )
;
