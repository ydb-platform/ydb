/* postgres can not */
SELECT
    ListFlatMap([1, 2, NULL], ($x) -> (10 + $x))
;
