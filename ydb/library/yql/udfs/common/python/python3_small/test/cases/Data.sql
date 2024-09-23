--sanitizer ignore memory 
$data = AsTuple(
    Bool("true"),
    Bool("FalsE"),
    Int8("-128"),
    Int8("127"),
    Uint8("0"),
    Uint8("255"),
    Int16("-32768"),
    Int16("32767"),
    Uint16("0"),
    Uint16("65535"),
    Int32("-2147483648"),
    Int32("2147483647"),
    Uint32("0"),
    Uint32("4294967295"),
    Int64("-9223372036854775808"),
    Int64("9223372036854775807"),
    Uint64("0"),
    Uint64("18446744073709551615"),
    Float("0"),
    Float("1"),
    Float("-1e30"),
    Float("-inf"),
    Float("+inf"),
    Float("nan"),
    Double("0"),
    Double("1"),
    Double("-1e300"),
    Double("-inf"),
    Double("+inf"),
    Double("nan"),
    String("foo\xffbar"),
    Utf8("привет"),
    Yson("<a=1>[3;%false]"),
    Json(@@{"a":1,"b":null}@@),
    Date("2000-01-01"),
    Datetime("2000-01-01T01:02:03Z"),
    Timestamp("2000-01-01T01:02:03.4Z"),
    Interval("P1DT12H"),
    TzDate("2000-01-01,Europe/Moscow"),
    TzDatetime("2000-01-01T01:02:03,Europe/Moscow"),
    TzTimestamp("2000-01-01T01:02:03.4,Europe/Moscow"),
    Uuid('31323334-3536-3738-393a-3b3c3d3e3f40'),
    Decimal('3.1415926535897932384626433832795029', 35, 34),
    Decimal('-.00000000000000000000000000000000001', 35, 35),
    Decimal('NAN', 10, 5),
    Decimal('-iNf', 1, 0)
);

$type = CallableType(0,
   TypeOf($data),
   TypeOf($data)
);

$f = Python3::f($type, @@
def f(x):
   return x
@@);

select $data, $f($data);
