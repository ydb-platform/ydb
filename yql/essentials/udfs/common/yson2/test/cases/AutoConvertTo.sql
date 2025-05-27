$zero = Yson::Parse("0u");
$bool = Yson::FromBool(true);
$int = Yson::Parse("123");
$uint = Yson::Parse("123u");
$negative = Yson::Parse("-123");
$double = Yson::Parse("123.456");
$string = Yson::Parse("\"123\"");

$options = Yson::Options(true AS AutoConvert);

SELECT
    Yson::ConvertToBool($zero, $options) AS zero_to_bool,
    Yson::ConvertToBool($bool, $options) AS bool_to_bool,
    Yson::ConvertToBool($int, $options) AS int_to_bool,
    Yson::ConvertToBool($uint, $options) AS uint_to_bool,
    Yson::ConvertToBool($negative, $options) AS negative_to_bool,
    Yson::ConvertToBool($double, $options) AS double_to_bool,
    Yson::ConvertToBool($string, $options) AS string_to_bool,

    Yson::ConvertToInt64($zero, $options) AS zero_to_int,
    Yson::ConvertToInt64($bool, $options) AS bool_to_int,
    Yson::ConvertToInt64($int, $options) AS int_to_int,
    Yson::ConvertToInt64($uint, $options) AS uint_to_int,
    Yson::ConvertToInt64($negative, $options) AS negative_to_int,
    Yson::ConvertToInt64($double, $options) AS double_to_int,
    Yson::ConvertToInt64($string, $options) AS string_to_int,

    Yson::ConvertToUint64($zero, $options) AS zero_to_uint,
    Yson::ConvertToUint64($bool, $options) AS bool_to_uint,
    Yson::ConvertToUint64($int, $options) AS int_to_uint,
    Yson::ConvertToUint64($uint, $options) AS uint_to_uint,
    Yson::ConvertToUint64($negative, $options) AS negative_to_uint,
    Yson::ConvertToUint64($double, $options) AS double_to_uint,
    Yson::ConvertToUint64($string, $options) AS string_to_uint,

    Yson::ConvertToDouble($zero, $options) AS zero_to_double,
    Yson::ConvertToDouble($bool, $options) AS bool_to_double,
    Yson::ConvertToDouble($int, $options) AS int_to_double,
    Yson::ConvertToDouble($uint, $options) AS uint_to_double,
    Yson::ConvertToDouble($negative, $options) AS negative_to_double,
    Yson::ConvertToDouble($double, $options) AS double_to_double,
    Yson::ConvertToDouble($string, $options) AS string_to_double,

    Yson::ConvertToString($zero, $options) AS zero_to_string,
    Yson::ConvertToString($bool, $options) AS bool_to_string,
    Yson::ConvertToString($int, $options) AS int_to_string,
    Yson::ConvertToString($uint, $options) AS uint_to_string,
    Yson::ConvertToString($negative, $options) AS negative_to_string,
    Yson::ConvertToString($double, $options) AS double_to_string,
    Yson::ConvertToString($string, $options) AS string_to_string,

    Yson::ConvertTo($string, Struct<x:Double?, y:Int64, z:List<Bool>>, $options) AS struct_stub,
    Yson::ConvertTo($double, Tuple<Double?, Int64, List<Bool>>, $options) AS tuple_stub;
