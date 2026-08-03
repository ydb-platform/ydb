SELECT
    Block(
        ($arg) -> {
            $dict = ListFold(
                [1, 2, 3, 2, 1],
                ToMutDict({0: 0}, $arg),
                ($item, $state) -> {
                    RETURN MutDictInsert($state, $item, 1);
                }
            );
            RETURN ListSort(DictKeys(FromMutDict($dict)));
        }
    )
;
