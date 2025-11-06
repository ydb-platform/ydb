/* custom error: The linear value has already been used */
$x = ToMutDict({1: 2}, 0);

SELECT
    FromMutDict(MutDictInsert($x, 3, 4)),
    FromMutDict(MutDictInsert($x, 5, 6))
;
