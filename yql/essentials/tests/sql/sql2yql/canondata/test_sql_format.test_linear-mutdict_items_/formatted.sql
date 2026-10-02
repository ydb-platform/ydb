SELECT
    Block(
        ($parent) -> {
            $d, $r = MutDictItems(
                ToMutDict({'a': 1, 'b': 2}, $parent)
            );
            RETURN (
                ListSort(DictItems(FromMutDict($d))),
                ListSort($r)
            );
        }
    )
;
