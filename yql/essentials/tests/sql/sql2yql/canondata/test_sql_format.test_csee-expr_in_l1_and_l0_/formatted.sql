SELECT
    AsList(
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN $x + (1 + 2);
            }
        ),
        AsList(1 + 2)
    )
;
