$all = [@@"str"@@y, "-13"y, "42u"y, "3.14"y, "#"y, "%false"y, "[1;2;3;]"y, "{}"y];

select
    Yson::IsString(y) as is_string,
    Yson::IsInt64(y) as is_int64,
    Yson::IsUint64(y) as is_uint64,
    Yson::IsDouble(y) as is_double,
    Yson::IsEntity(y) as is_entity,
    Yson::IsBool(y) as is_bool,
    Yson::IsList(y) as is_list,
    Yson::IsDict(y) as is_dict
FROM AS_TABLE(ListMap($all, ($y)->(<|'y':$y|>)));
