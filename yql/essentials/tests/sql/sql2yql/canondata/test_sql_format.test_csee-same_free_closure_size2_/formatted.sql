SELECT
    AsTuple(
        YQL::FlatMap(
            3 / 1, ($x) -> {
                RETURN YQL::FlatMap(
                    1 / 1, ($y) -> {
                        RETURN Just($x + $y);
                    }
                );
            }
        ),
        YQL::FlatMap(
            4 / 1, ($x) -> {
                RETURN YQL::FlatMap(
                    1 / 1, ($y) -> {
                        RETURN Just($x + $y);
                    }
                );
            }
        )
    )
;
