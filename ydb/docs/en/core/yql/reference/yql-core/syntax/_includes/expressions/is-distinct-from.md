## IS \[NOT\] DISTINCT FROM {#is-distinct-from}

Comparing of two values. Unlike the regular [comparison operators](#comparison-operators), NULLs are treated as equal to each other.
More precisely, the comparison is carried out according to the following rules:

1) The operators `IS DISTINCT FROM`/`IS NOT DISTINCT FROM` are defined for those and only for those arguments for which the operators `!=` and `=` are defined.
2) The result of `IS NOT DISTINCT FROM` is equal to the logical negation of the `IS DISTINCT FROM` result for these arguments.
3) If the result of the `==` operator is not equal to zero for some arguments, then it is equal to the result of the `IS NOT DISTINCT FROM` operator for the same arguments.
4) If both arguments are empty `Optional` or `NULL`s, then the value of `IS NOT DISTINCT FROM` is `True`.
5) The result of `IS NOT DISTINCT FROM` for an empty `Optional` or `NULL` and filled-in `Optional` or non-`Optional` value is `False`.

For values of composite types, these rules are used recursively.

