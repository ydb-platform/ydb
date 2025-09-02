SELECT
    AsList(
        AsList(1 + 2),
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + (1 + 2);
            }
        ),
    )
;
