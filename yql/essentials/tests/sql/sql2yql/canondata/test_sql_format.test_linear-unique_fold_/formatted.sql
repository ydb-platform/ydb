SELECT
    ListSort(
        DictKeys(
            Block(
                ($p) -> {
                    RETURN FromMutDict(
                        FromDynamicLinear(
                            ListFold(
                                [1, 2, 1], ToDynamicLinear(MutDictCreate(Int32, Void, $p)), ($item, $state) -> {
                                    RETURN ToDynamicLinear(MutDictInsert(FromDynamicLinear($state), $item, Void()));
                                }
                            )
                        )
                    );
                }
            )
        )
    )
;
