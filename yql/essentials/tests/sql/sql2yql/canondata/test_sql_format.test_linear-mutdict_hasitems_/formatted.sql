SELECT
    Block(
        ($parent) -> {
            $d, $r = MutDictHasItems(
                ToMutDict({'a': 1, 'b': 2}, $parent)
            );
            RETURN (
                ListSort(DictItems(FromMutDict($d))),
                $r
            );
        }
    )
;
