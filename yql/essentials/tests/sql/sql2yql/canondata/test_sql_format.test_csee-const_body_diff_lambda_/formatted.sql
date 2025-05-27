SELECT
    AsTuple(
        ListMap(
            AsList(1, 2), ($_x) -> {
                RETURN 3 + 4;
            }
        ),
        ListMap(
            AsList('foo', 'bar'), ($_x) -> {
                RETURN 3 + 4;
            }
        )
    )
;
