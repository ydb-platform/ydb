$maximum = Decimal('99999999999999999999999999999999999', 35, 0);
$minimum = Decimal('-99999999999999999999999999999999999', 35, 0);
$epsilon = Decimal('0.00000000000000000000000000000000001', 35, 35);
$negative_epsilon = Decimal('-0.00000000000000000000000000000000001', 35, 35);
$right_maximum = Decimal('0.99999999999999999999999999999999999', 35, 35);
$right_minimum = Decimal('-0.99999999999999999999999999999999999', 35, 35);

$cases = [
    <|
        name: 'positive overflow boundary', lhs: $maximum, rhs: $epsilon,
        equals: FALSE, not_equals: TRUE, less: FALSE, less_or_equal: FALSE, greater: TRUE, greater_or_equal: TRUE
    |>,
    <|
        name: 'negative overflow boundary', lhs: $minimum, rhs: $negative_epsilon,
        equals: FALSE, not_equals: TRUE, less: TRUE, less_or_equal: TRUE, greater: FALSE, greater_or_equal: FALSE
    |>,
    <|
        name: 'zero', lhs: Decimal('0', 35, 0), rhs: Decimal('0', 35, 35),
        equals: TRUE, not_equals: FALSE, less: FALSE, less_or_equal: TRUE, greater: FALSE, greater_or_equal: TRUE
    |>,
    <|
        name: 'signed zero', lhs: Decimal('-0', 35, 0), rhs: Decimal('+0', 35, 35),
        equals: TRUE, not_equals: FALSE, less: FALSE, less_or_equal: TRUE, greater: FALSE, greater_or_equal: TRUE
    |>,
    <|
        name: 'maximum below infinity', lhs: $maximum, rhs: Decimal('inf', 35, 35),
        equals: FALSE, not_equals: TRUE, less: TRUE, less_or_equal: TRUE, greater: FALSE, greater_or_equal: FALSE
    |>,
    <|
        name: 'infinity above maximum', lhs: Decimal('inf', 35, 0), rhs: $right_maximum,
        equals: FALSE, not_equals: TRUE, less: FALSE, less_or_equal: FALSE, greater: TRUE, greater_or_equal: TRUE
    |>,
    <|
        name: 'minimum above negative infinity', lhs: $minimum, rhs: Decimal('-inf', 35, 35),
        equals: FALSE, not_equals: TRUE, less: FALSE, less_or_equal: FALSE, greater: TRUE, greater_or_equal: TRUE
    |>,
    <|
        name: 'negative infinity below minimum', lhs: Decimal('-inf', 35, 0), rhs: $right_minimum,
        equals: FALSE, not_equals: TRUE, less: TRUE, less_or_equal: TRUE, greater: FALSE, greater_or_equal: FALSE
    |>,
    <|
        name: 'unordered left', lhs: Decimal('nan', 35, 0), rhs: Decimal('0', 35, 35),
        equals: FALSE, not_equals: TRUE, less: FALSE, less_or_equal: FALSE, greater: FALSE, greater_or_equal: FALSE
    |>,
    <|
        name: 'unordered right', lhs: Decimal('0', 35, 0), rhs: Decimal('nan', 35, 35),
        equals: FALSE, not_equals: TRUE, less: FALSE, less_or_equal: FALSE, greater: FALSE, greater_or_equal: FALSE
    |>,
];

SELECT
    name,
    equals AS expected_equals,
    lhs == rhs AS actual_equals,
    not_equals AS expected_not_equals,
    lhs != rhs AS actual_not_equals,
    less AS expected_less,
    lhs < rhs AS actual_less,
    less_or_equal AS expected_less_or_equal,
    lhs <= rhs AS actual_less_or_equal,
    greater AS expected_greater,
    lhs > rhs AS actual_greater,
    greater_or_equal AS expected_greater_or_equal,
    lhs >= rhs AS actual_greater_or_equal
FROM
    AS_TABLE($cases)
ORDER BY
    name
;
