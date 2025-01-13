SELECT
    AsList(
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + 1;
            }
        ),
        ListMap(
            AsList(10, 11), ($x) -> {
                RETURN $x + 1;
            }
        )
    )
;
