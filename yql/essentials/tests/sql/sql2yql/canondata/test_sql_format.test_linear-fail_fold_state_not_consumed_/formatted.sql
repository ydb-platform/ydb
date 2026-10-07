/* custom error: Linear value is not consumed */
SELECT
    FromMutDict(
        ListFold(
            [1, 2],
            (ToMutDict({0: 0}, NULL), 0),
            ($item, $state) -> {
                RETURN (ToMutDict({0: 0}, $item), $item);
            }
        ).0
    )
;
