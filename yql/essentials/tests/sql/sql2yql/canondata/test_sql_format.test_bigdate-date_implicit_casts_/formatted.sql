PRAGMA warning('disable', '4528');

$fromTypes = {
    'Date32': '2025-03-19',
    'Datetime64': '2025-03-19T01:02:03Z',
    'Timestamp64': '2025-03-19T01:02:03.456789Z',
};

$toTypes = [
    'Date', 'Datetime', 'Timestamp',
    'TzDate', 'TzDatetime', 'TzTimestamp',
    'Date32', 'Datetime64', 'Timestamp64',
    'TzDate32', 'TzDatetime64', 'TzTimestamp64',
];

$allowed = {
    'Date32': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */'DateTime64', 'Timestamp64',
        'TzDate32', 'TzDatetime64', 'TzTimestamp64',
    },
    'Datetime64': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */'Timestamp64',

        /* - */ 'TzDatetime64', 'TzTimestamp64',
    },
    'Timestamp64': {
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */ /* - */
        /* - */ /* - */'TzTimestamp64',
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
