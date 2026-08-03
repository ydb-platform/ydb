/* custom error: An argument of a lambda should not be a linear type */
$d = ToMutDict({1: 2}, NULL);
$t = TypeOf($d);

SELECT
    Callable(
        Callable<($t) -> Uint64>, ($x) -> {
            RETURN DictLength(FromMutDict($x));
        }
    )($d)
;
