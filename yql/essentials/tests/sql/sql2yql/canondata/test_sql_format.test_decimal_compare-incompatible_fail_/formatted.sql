/* custom error: Uncompatible types in compare: Decimal(3,0) '<' String */
$cases = [<|lhs: Decimal('1', 3, 0), rhs: '1'|>];

SELECT
    lhs < rhs
FROM
    AS_TABLE($cases)
;
