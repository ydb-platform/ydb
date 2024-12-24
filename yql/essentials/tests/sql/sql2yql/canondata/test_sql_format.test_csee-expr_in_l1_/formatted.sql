SELECT
    AsList(
        ListMap(
            AsList(1, 2), ($x) -> {
                RETURN ($x + 1) * ($x + 1);
            }
        ),
    )
;
