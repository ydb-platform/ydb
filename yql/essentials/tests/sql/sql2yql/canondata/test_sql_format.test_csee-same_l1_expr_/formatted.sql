SELECT
    AsList(
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + 1;
            }
        ),
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + 1;
            }
        )
    )
;
