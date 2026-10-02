/* custom error: Linear value is not consumed */
$d1 = ToMutDict({1}, 1);
$d2 = ToMutDict({2}, 2);

SELECT
    FromMutDict(
        FromDynamicLinear(
            Unwrap(
                Opaque(
                    [
                        ToDynamicLinear($d1),
                        ToDynamicLinear($d2)
                    ]
                )[0]
            )
        )
    )
;
