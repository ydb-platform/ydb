/* custom error: The linear value has already been used */
SELECT
    Block(
        ($parent) -> {
            $list = Opaque([ToDynamicLinear(ToMutDict({'a': 1}, $parent))]);
            $a, $b = (Unwrap(ListHead($list)), Unwrap(ListLast($list)));
            RETURN (FromMutDict(FromDynamicLinear($a)), FromMutDict(FromDynamicLinear($b)));
        }
    )
;
