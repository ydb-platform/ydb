/* custom error: The linear value changed lambda scope */
$b = {4: 5};

SELECT
    ($x) -> (
        Block(
            ($a) -> {
                $d = ToMutDict($b, $a);
                $d = MutDictUpsert($d, $x, 1);
                RETURN FromMutDict($d);
            }
        )
    )(a)
FROM
    AS_TABLE(Opaque([<|a: 1|>, <|a: 2|>, <|a: 3|>]))
;
