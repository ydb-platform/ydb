PRAGMA warning('disable', '4528');

$fromTypes = {
    'TzDate32': '2025-03-19,Europe/Moscow',
    'TzDatetime64': '2025-03-19T01:02:03,Europe/Moscow',
    'TzTimestamp64': '2025-03-19T01:02:03.456789,Europe/Moscow',
};

$toTypes = [
    'Date', 'Datetime', 'Timestamp',
    'TzDate', 'TzDatetime', 'TzTimestamp',
    'Date32', 'Datetime64', 'Timestamp64',
    'TzDate32', 'TzDatetime64', 'TzTimestamp64',
];

$allowed = {
    'TzDate32': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */'TzDatetime64', 'TzTimestamp64',
    },
    'TzDatetime64': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */'TzTimestamp64',
    },
    'TzTimestamp64': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
    },
};

EVALUATE FOR $from IN DictItems($fromTypes) DO BEGIN
    EVALUATE FOR $to IN $toTypes DO BEGIN
        EVALUATE IF DictContains($allowed[$from.0], $to) DO BEGIN
            $callable = Callable(CallableType(0, String, DataType($to)), ($x) -> (CAST($x AS String)));
            $srcType = DataType($from.0);

            SELECT
                $from.0 || ' => ' || $to,
                $callable(Unwrap(CAST($from.1 AS $srcType)))
            ;
        END DO;
    END DO;
END DO;
