$makeFreq = ($list) -> (
    Block(
        ($x) -> {
            $itemType = ListItemType(TypeOf($list));
            $initDict = MutDictCreate($itemType, Uint32, $x);
            RETURN FromMutDict(
                ListFold(
                    $list, $initDict, ($item, $state) -> {
                        $linear, $prevFreq = MutDictLookup($state, $item);
                        $linear = MutDictUpsert($linear, $item, ($prevFreq ?? 0u) + 1u);
                        RETURN $linear;
                    }
                )
            );
        }
    )
);

SELECT
    $makeFreq([1, 2, 1, 3, 1])
;
