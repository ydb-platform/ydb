SELECT
    ListSort(
        DictItems(
            Block(
                ($parent) -> (
                    FromMutDict(
                        FromDynamicLinear(Unwrap(ListHead([ToDynamicLinear(ToMutDict({'a': 1, 'b': 2}, $parent))])))
                    )
                )
            )
        )
    )
;
