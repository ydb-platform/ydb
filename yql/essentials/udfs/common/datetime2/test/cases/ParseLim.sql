$dt_parser1 = DateTime::Parse('%Y-%m-%d');
$dt_parser2 = DateTime::Parse('%Y-%m-%d %H:%M:%S');
$dt_parser1z = DateTime::Parse('%Y-%m-%d %Z');
$dt_parser2z = DateTime::Parse('%Y-%m-%d %H:%M:%S %Z');

SELECT
    $dt_parser1("2105-12-31"), $dt_parser1("2106-01-01"),
    $dt_parser2("2105-12-31 23:59:59"), $dt_parser2("2106-01-01 00:00:00"),
    $dt_parser2("2105-12-31 23:59:59.999999"), $dt_parser2("2106-01-01 00:00:00.000000"),
    $dt_parser1z("2105-12-31 Etc/GMT+11"),
    $dt_parser1z("2106-01-01 Etc/GMT-1"),
    $dt_parser2z("2105-12-31 23:00:00 Etc/GMT+1"),
    $dt_parser2z("2105-12-31 22:59:59.999999 Etc/GMT+1"),
    $dt_parser1("1970-01-01"), $dt_parser1("1969-12-31"),
    $dt_parser2("1970-01-01 00:00:00"), $dt_parser2("1969-12-31 23:59:59"),
    $dt_parser2("1969-12-31 23:59:59.999999"), $dt_parser2("1970-01-01 00:00:00.000000"),
    $dt_parser2z("1969-12-31 23:00:00 Etc/GMT+1"),
    $dt_parser2z("1969-12-31 22:59:59.999999 Etc/GMT+1");

