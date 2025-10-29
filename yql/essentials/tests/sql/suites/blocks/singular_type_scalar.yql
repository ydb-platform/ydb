PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

-- Test data for Void type
$void_data = [
            <|vid: Just(Void())|>,
            <|vid: null|>
        ];

-- Test data for Tuple type
$tuple_data = [
            <|empty_tuple: Just(AsTuple())|>,
            <|empty_tuple: null|>
        ];

-- Test data for Struct type
$struct_data = [
            <|empty_struct: Just(AsStruct())|>,
            <|empty_struct: null|>
        ];

-- Test data for Null type
$null_data = [
            <|nil: Just(NULL)|>,
            <|nil: null|>
        ];

-- Test data for List type
$list_data = [
            <|empty_list: Just([])|>,
            <|empty_list: null|>
        ];

-- Test Void type
select Coalesce(vid, Void()) as void_result from AS_TABLE($void_data);

-- Test Tuple type
select Coalesce(empty_tuple, AsTuple()) as tuple_result from AS_TABLE($tuple_data);

-- Test Struct type
select Coalesce(empty_struct, AsStruct()) as struct_result from AS_TABLE($struct_data);

-- Test Null type
select Coalesce(nil, NULL) as null_result from AS_TABLE($null_data);

-- Test List type
select Coalesce(empty_list, []) as list_result from AS_TABLE($list_data);
