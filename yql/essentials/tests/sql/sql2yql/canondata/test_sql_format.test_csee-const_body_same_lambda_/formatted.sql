SELECT
    AsTuple(
        ListMap(
            AsList(1, 2), ($_x) -> {
                RETURN 3 + 4;
            }
        ),
        ListMap(
            AsList(5, 6), ($_x) -> {
                RETURN 3 + 4;
            }
        )
    )
;
