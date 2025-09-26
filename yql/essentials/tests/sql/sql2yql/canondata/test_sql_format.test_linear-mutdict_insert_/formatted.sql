SELECT
    ListSort(
        DictItems(
            Block(
                ($parent) -> (
                    FromMutDict(
                        MutDictInsert(
                            ToMutDict({'a': 1, 'b': 2}, $parent),
                            'c', 3
                        )
                    )
                )
            )
        )
    )
;
