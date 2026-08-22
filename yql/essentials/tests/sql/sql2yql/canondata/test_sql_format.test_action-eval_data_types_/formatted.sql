$data = (
    TRUE,
    1t,
    2ut,
    3s,
    4us,
    5,
    6u,
    7l,
    8ul,
    9.0,
    10.0f,
    Decimal('11.3', 5, 2),
    '\xff\xff',
    "bar"u,
    "{a=1}"y,
    "[1,2,3]"j,
    Date('2020-01-01'),
    Datetime('2020-01-01T01:02:03Z'),
    Timestamp('2020-01-01T01:02:03.456789Z'),
    TzDate('2020-01-01,Europe/Moscow'),
    TzDatetime('2020-01-01T01:02:03,Europe/Moscow'),
    TzTimestamp('2020-01-01T01:02:03.456789,Europe/Moscow'),
    Interval('P1D'),
    Date32('2020-01-01'),
    Datetime64('2020-01-01T01:02:03Z'),
    Timestamp64('2020-01-01T01:02:03.456789Z'),
    TzDate32('2020-01-01,Europe/Moscow'),
    TzDatetime64('2020-01-01T01:02:03,Europe/Moscow'),
    TzTimestamp64('2020-01-01T01:02:03.456789,Europe/Moscow'),
    Interval64('P1D'),
    DyNumber('12.4'),
    JsonDocument('[1,2,3]')
);

$tc = TupleTypeComponents(TypeHandle(TypeOf($data)));

SELECT
    EvaluateCode(
        ListCode(
            ListMap(
                ListEnumerate($tc), ($x) -> {
                    RETURN FuncCode('Nth', ReprCode($data), AtomCode(CAST($x.0 AS String)));
                }
            )
        )
    )
;
