SELECT
    Block(
        ($parent) -> {
            $d, $r = MutDictContains(
                ToMutDict({'a': 1, 'b': 2}, $parent),
                'a'
            );
            RETURN (
                ListSort(DictItems(FromMutDict($d))),
                $r
            );
        }
    )
;
