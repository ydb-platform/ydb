/* custom error: A lambda body should not be a linear type */
SELECT
    FromMutDict(Block(($_) -> (ToMutDict({1: 2}, 0))))
;
