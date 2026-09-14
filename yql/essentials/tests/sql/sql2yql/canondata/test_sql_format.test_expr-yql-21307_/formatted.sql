SELECT
    DictAggregate(
        {}, ListMap(
            [(1)(2), (3)], ($_) -> {
                RETURN (4, 5);
            }
        )
    )
;
