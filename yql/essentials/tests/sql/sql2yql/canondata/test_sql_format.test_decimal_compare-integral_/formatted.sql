$cases = [
    <|
        name: 'i8', expected: TRUE,
        actual: Decimal('127', 35, 15) == Int8('127')
    |>,
    <|
        name: 'ui8', expected: TRUE,
        actual: Uint8('255') == Decimal('255', 35, 15)
    |>,
    <|
        name: 'i16', expected: TRUE,
        actual: Decimal('32767', 35, 15) == Int16('32767')
    |>,
    <|
        name: 'ui16', expected: TRUE,
        actual: Uint16('65535') == Decimal('65535', 35, 15)
    |>,
    <|
        name: 'i32', expected: TRUE,
        actual: Decimal('2147483647', 35, 15) == Int32('2147483647')
    |>,
    <|
        name: 'ui32', expected: TRUE,
        actual: Uint32('4294967295') == Decimal('4294967295', 35, 15)
    |>,
    <|
        name: 'i64', expected: TRUE,
        actual: Decimal('9223372036854775807', 35, 15) == Int64('9223372036854775807')
    |>,
    <|
        name: 'ui64', expected: TRUE,
        actual: Uint64('18446744073709551615') == Decimal('18446744073709551615', 35, 15)
    |>,
    <|
        name: 'i64 below infinity', expected: TRUE,
        actual: Int64('9223372036854775807') < Decimal('inf', 20, 18)
    |>,
    <|
        name: 'infinity above i64', expected: TRUE,
        actual: Decimal('inf', 20, 18) > Int64('9223372036854775807')
    |>,
    <|
        name: 'ui64 below infinity', expected: TRUE,
        actual: Uint64('18446744073709551615') < Decimal('inf', 20, 18)
    |>,
    <|
        name: 'infinity above ui64', expected: TRUE,
        actual: Decimal('inf', 20, 18) > Uint64('18446744073709551615')
    |>,
];

SELECT
    name,
    expected,
    actual
FROM
    AS_TABLE($cases)
ORDER BY
    name
;
