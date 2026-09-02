/* custom error: The linear value has already been used */
SELECT
    Block(
        ($arg) -> {
            $dict = ListFold(
                [2],
                ToMutDict({0: 0}, $arg),
                ($item, $state) -> {
                    RETURN MutDictInsert($state, UnWrap(DictLookup(FromMutDict($state), 0)), 1);
                }
            );
            RETURN ListSort(DictKeys(FromMutDict($dict)));
        }
    )
;
