PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

-- Test data for Void type
$void_data = [
    <|vid: Just(Void())|>,
    <|vid: NULL|>
];

-- Test data for Tuple type
$tuple_data = [
    <|empty_tuple: Just(AsTuple())|>,
    <|empty_tuple: NULL|>
];

-- Test data for Struct type
$struct_data = [
    <|empty_struct: Just(AsStruct())|>,
    <|empty_struct: NULL|>
];

-- Test data for Null type
$null_data = [
    <|nil: Just(NULL)|>,
    <|nil: NULL|>
];

-- Test data for List type
$list_data = [
    <|empty_list: Just([])|>,
    <|empty_list: NULL|>
];

-- Test Void type
SELECT
    Coalesce(vid, Void()) AS void_result
FROM
    AS_TABLE($void_data)
;

-- Test Tuple type
SELECT
    Coalesce(empty_tuple, AsTuple()) AS tuple_result
FROM
    AS_TABLE($tuple_data)
;

-- Test Struct type
SELECT
    Coalesce(empty_struct, AsStruct()) AS struct_result
FROM
    AS_TABLE($struct_data)
;

-- Test Null type
SELECT
    Coalesce(nil, NULL) AS null_result
FROM
    AS_TABLE($null_data)
;

-- Test List type
SELECT
    Coalesce(empty_list, []) AS list_result
FROM
    AS_TABLE($list_data)
;
