SELECT
    ListSort(
        DictItems(
            Block(
                ($parent) -> (
                    FromMutDict(
                        MutDictRemove(
                            ToMutDict({'a': 1, 'b': 2}, $parent),
                            'a'
                        )
                    )
                )
            )
        )
    )
;
