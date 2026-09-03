$empty_left = Nothing(Optional<Decimal (3, 2)>);
$empty_right = Nothing(Optional<Decimal (5, 4)>);

$cases = [
    <|
        name: 'equal', lhs: Just(Decimal('-1.23', 3, 2)), rhs: Just(Decimal('-1.2300', 5, 4)),
        equals: Just(TRUE), not_equals: Just(FALSE), less: Just(FALSE), less_or_equal: Just(TRUE),
        greater: Just(FALSE), greater_or_equal: Just(TRUE)
    |>,
    <|
        name: 'greater', lhs: Just(Decimal('-1.23', 3, 2)), rhs: Just(Decimal('-1.2301', 5, 4)),
        equals: Just(FALSE), not_equals: Just(TRUE), less: Just(FALSE), less_or_equal: Just(FALSE),
        greater: Just(TRUE), greater_or_equal: Just(TRUE)
    |>,
    <|
        name: 'empty left', lhs: $empty_left, rhs: Just(Decimal('-1.2300', 5, 4)),
        equals: Nothing(Optional<Bool>), not_equals: Nothing(Optional<Bool>),
        less: Nothing(Optional<Bool>), less_or_equal: Nothing(Optional<Bool>),
        greater: Nothing(Optional<Bool>), greater_or_equal: Nothing(Optional<Bool>)
    |>,
    <|
        name: 'both empty', lhs: $empty_left, rhs: $empty_right,
        equals: Nothing(Optional<Bool>), not_equals: Nothing(Optional<Bool>),
        less: Nothing(Optional<Bool>), less_or_equal: Nothing(Optional<Bool>),
        greater: Nothing(Optional<Bool>), greater_or_equal: Nothing(Optional<Bool>)
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
