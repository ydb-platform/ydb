/* custom error: An argument of a lambda should not be a linear type */
SELECT
    FromMutDict(ListFold([1, 2], ToMutDict({1: 2}, 0), ($_, $x) -> ($x)))
;
