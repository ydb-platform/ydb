SELECT
    ListSort(
        DictItems(
            Block(
                ($parent) -> (
                    FromMutDict(
                        MutDictUpsert(
                            ToMutDict({'a': 1, 'b': 2}, $parent),
                            'c', 3
                        )
                    )
                )
            )
        )
    )
;
