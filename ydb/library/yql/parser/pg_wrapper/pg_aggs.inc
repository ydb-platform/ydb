auto MakePgAgg_max_2050() {
    return TGenericAgg<
                     TPgDirectFunc<&array_larger>, true, TArgs_array_larger_Policy,
                     TPgDirectFunc<&array_larger>, true, TArgs_array_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2051() {
    return TGenericAgg<
                     TPgDirectFunc<&array_smaller>, true, TArgs_array_smaller_Policy,
                     TPgDirectFunc<&array_smaller>, true, TArgs_array_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_avg_2100() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_avg_accum>, false, TArgs_int8_avg_accum_Policy,
                     TPgDirectFunc<&int8_avg_combine>, false, TArgs_int8_avg_combine_Policy,
                     true, TPgDirectFunc<&int8_avg_serialize>, TArgs_int8_avg_serialize_Policy,
                     true, TPgDirectFunc<&int8_avg_deserialize>, TArgs_int8_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_avg>, false, TArgs_numeric_poly_avg_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_avg_2101() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_avg_accum>, true, TArgs_int4_avg_accum_Policy,
                     TPgDirectFunc<&int4_avg_combine>, true, TArgs_int4_avg_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&int8_avg>, true, TArgs_int8_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_2102() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_avg_accum>, true, TArgs_int2_avg_accum_Policy,
                     TPgDirectFunc<&int4_avg_combine>, true, TArgs_int4_avg_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&int8_avg>, true, TArgs_int8_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_2103() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_avg_accum>, false, TArgs_numeric_avg_accum_Policy,
                     TPgDirectFunc<&numeric_avg_combine>, false, TArgs_numeric_avg_combine_Policy,
                     true, TPgDirectFunc<&numeric_avg_serialize>, TArgs_numeric_avg_serialize_Policy,
                     true, TPgDirectFunc<&numeric_avg_deserialize>, TArgs_numeric_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_avg>, false, TArgs_numeric_avg_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_avg_2104() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_avg>, true, TArgs_float8_avg_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_2105() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_avg>, true, TArgs_float8_avg_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_2106() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_accum>, true, TArgs_interval_accum_Policy,
                     TPgDirectFunc<&interval_combine>, true, TArgs_interval_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&interval_avg>, true, TArgs_interval_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_sum_2107() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_avg_accum>, false, TArgs_int8_avg_accum_Policy,
                     TPgDirectFunc<&int8_avg_combine>, false, TArgs_int8_avg_combine_Policy,
                     true, TPgDirectFunc<&int8_avg_serialize>, TArgs_int8_avg_serialize_Policy,
                     true, TPgDirectFunc<&int8_avg_deserialize>, TArgs_int8_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_sum>, false, TArgs_numeric_poly_sum_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_sum_2108() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_sum>, false, TArgs_int4_sum_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2109() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_sum>, false, TArgs_int2_sum_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2110() {
    return TGenericAgg<
                     TPgDirectFunc<&float4pl>, true, TArgs_float4pl_Policy,
                     TPgDirectFunc<&float4pl>, true, TArgs_float4pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2111() {
    return TGenericAgg<
                     TPgDirectFunc<&float8pl>, true, TArgs_float8pl_Policy,
                     TPgDirectFunc<&float8pl>, true, TArgs_float8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2112() {
    return TGenericAgg<
                     TPgDirectFunc<&cash_pl>, true, TArgs_cash_pl_Policy,
                     TPgDirectFunc<&cash_pl>, true, TArgs_cash_pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2113() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_pl>, true, TArgs_interval_pl_Policy,
                     TPgDirectFunc<&interval_pl>, true, TArgs_interval_pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2114() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_avg_accum>, false, TArgs_numeric_avg_accum_Policy,
                     TPgDirectFunc<&numeric_avg_combine>, false, TArgs_numeric_avg_combine_Policy,
                     true, TPgDirectFunc<&numeric_avg_serialize>, TArgs_numeric_avg_serialize_Policy,
                     true, TPgDirectFunc<&numeric_avg_deserialize>, TArgs_numeric_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_sum>, false, TArgs_numeric_sum_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_max_2115() {
    return TGenericAgg<
                     TPgDirectFunc<&int8larger>, true, TArgs_int8larger_Policy,
                     TPgDirectFunc<&int8larger>, true, TArgs_int8larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2116() {
    return TGenericAgg<
                     TPgDirectFunc<&int4larger>, true, TArgs_int4larger_Policy,
                     TPgDirectFunc<&int4larger>, true, TArgs_int4larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2117() {
    return TGenericAgg<
                     TPgDirectFunc<&int2larger>, true, TArgs_int2larger_Policy,
                     TPgDirectFunc<&int2larger>, true, TArgs_int2larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2118() {
    return TGenericAgg<
                     TPgDirectFunc<&oidlarger>, true, TArgs_oidlarger_Policy,
                     TPgDirectFunc<&oidlarger>, true, TArgs_oidlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2119() {
    return TGenericAgg<
                     TPgDirectFunc<&float4larger>, true, TArgs_float4larger_Policy,
                     TPgDirectFunc<&float4larger>, true, TArgs_float4larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2120() {
    return TGenericAgg<
                     TPgDirectFunc<&float8larger>, true, TArgs_float8larger_Policy,
                     TPgDirectFunc<&float8larger>, true, TArgs_float8larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2122() {
    return TGenericAgg<
                     TPgDirectFunc<&date_larger>, true, TArgs_date_larger_Policy,
                     TPgDirectFunc<&date_larger>, true, TArgs_date_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2123() {
    return TGenericAgg<
                     TPgDirectFunc<&time_larger>, true, TArgs_time_larger_Policy,
                     TPgDirectFunc<&time_larger>, true, TArgs_time_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2124() {
    return TGenericAgg<
                     TPgDirectFunc<&timetz_larger>, true, TArgs_timetz_larger_Policy,
                     TPgDirectFunc<&timetz_larger>, true, TArgs_timetz_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2125() {
    return TGenericAgg<
                     TPgDirectFunc<&cashlarger>, true, TArgs_cashlarger_Policy,
                     TPgDirectFunc<&cashlarger>, true, TArgs_cashlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2126() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2127() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2128() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_larger>, true, TArgs_interval_larger_Policy,
                     TPgDirectFunc<&interval_larger>, true, TArgs_interval_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2129() {
    return TGenericAgg<
                     TPgDirectFunc<&text_larger>, true, TArgs_text_larger_Policy,
                     TPgDirectFunc<&text_larger>, true, TArgs_text_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2130() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_larger>, true, TArgs_numeric_larger_Policy,
                     TPgDirectFunc<&numeric_larger>, true, TArgs_numeric_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2131() {
    return TGenericAgg<
                     TPgDirectFunc<&int8smaller>, true, TArgs_int8smaller_Policy,
                     TPgDirectFunc<&int8smaller>, true, TArgs_int8smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2132() {
    return TGenericAgg<
                     TPgDirectFunc<&int4smaller>, true, TArgs_int4smaller_Policy,
                     TPgDirectFunc<&int4smaller>, true, TArgs_int4smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2133() {
    return TGenericAgg<
                     TPgDirectFunc<&int2smaller>, true, TArgs_int2smaller_Policy,
                     TPgDirectFunc<&int2smaller>, true, TArgs_int2smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2134() {
    return TGenericAgg<
                     TPgDirectFunc<&oidsmaller>, true, TArgs_oidsmaller_Policy,
                     TPgDirectFunc<&oidsmaller>, true, TArgs_oidsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2135() {
    return TGenericAgg<
                     TPgDirectFunc<&float4smaller>, true, TArgs_float4smaller_Policy,
                     TPgDirectFunc<&float4smaller>, true, TArgs_float4smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2136() {
    return TGenericAgg<
                     TPgDirectFunc<&float8smaller>, true, TArgs_float8smaller_Policy,
                     TPgDirectFunc<&float8smaller>, true, TArgs_float8smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2138() {
    return TGenericAgg<
                     TPgDirectFunc<&date_smaller>, true, TArgs_date_smaller_Policy,
                     TPgDirectFunc<&date_smaller>, true, TArgs_date_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2139() {
    return TGenericAgg<
                     TPgDirectFunc<&time_smaller>, true, TArgs_time_smaller_Policy,
                     TPgDirectFunc<&time_smaller>, true, TArgs_time_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2140() {
    return TGenericAgg<
                     TPgDirectFunc<&timetz_smaller>, true, TArgs_timetz_smaller_Policy,
                     TPgDirectFunc<&timetz_smaller>, true, TArgs_timetz_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2141() {
    return TGenericAgg<
                     TPgDirectFunc<&cashsmaller>, true, TArgs_cashsmaller_Policy,
                     TPgDirectFunc<&cashsmaller>, true, TArgs_cashsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2142() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2143() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2144() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_smaller>, true, TArgs_interval_smaller_Policy,
                     TPgDirectFunc<&interval_smaller>, true, TArgs_interval_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2145() {
    return TGenericAgg<
                     TPgDirectFunc<&text_smaller>, true, TArgs_text_smaller_Policy,
                     TPgDirectFunc<&text_smaller>, true, TArgs_text_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2146() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_smaller>, true, TArgs_numeric_smaller_Policy,
                     TPgDirectFunc<&numeric_smaller>, true, TArgs_numeric_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_2147() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc_any>, true, TArgs_int8inc_any_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_variance_2148() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_2149() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_2150() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_2151() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_variance_2152() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_variance_2153() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_2154() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_2155() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_2156() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_2157() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_2158() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_2159() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_bit_and_2236() {
    return TGenericAgg<
                     TPgDirectFunc<&int2and>, true, TArgs_int2and_Policy,
                     TPgDirectFunc<&int2and>, true, TArgs_int2and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2237() {
    return TGenericAgg<
                     TPgDirectFunc<&int2or>, true, TArgs_int2or_Policy,
                     TPgDirectFunc<&int2or>, true, TArgs_int2or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2238() {
    return TGenericAgg<
                     TPgDirectFunc<&int4and>, true, TArgs_int4and_Policy,
                     TPgDirectFunc<&int4and>, true, TArgs_int4and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2239() {
    return TGenericAgg<
                     TPgDirectFunc<&int4or>, true, TArgs_int4or_Policy,
                     TPgDirectFunc<&int4or>, true, TArgs_int4or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2240() {
    return TGenericAgg<
                     TPgDirectFunc<&int8and>, true, TArgs_int8and_Policy,
                     TPgDirectFunc<&int8and>, true, TArgs_int8and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2241() {
    return TGenericAgg<
                     TPgDirectFunc<&int8or>, true, TArgs_int8or_Policy,
                     TPgDirectFunc<&int8or>, true, TArgs_int8or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2242() {
    return TGenericAgg<
                     TPgDirectFunc<&bit_and>, true, TArgs_bit_and_Policy,
                     TPgDirectFunc<&bit_and>, true, TArgs_bit_and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2243() {
    return TGenericAgg<
                     TPgDirectFunc<&bit_or>, true, TArgs_bit_or_Policy,
                     TPgDirectFunc<&bit_or>, true, TArgs_bit_or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2244() {
    return TGenericAgg<
                     TPgDirectFunc<&bpchar_larger>, true, TArgs_bpchar_larger_Policy,
                     TPgDirectFunc<&bpchar_larger>, true, TArgs_bpchar_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2245() {
    return TGenericAgg<
                     TPgDirectFunc<&bpchar_smaller>, true, TArgs_bpchar_smaller_Policy,
                     TPgDirectFunc<&bpchar_smaller>, true, TArgs_bpchar_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_array_agg_2335() {
    return TGenericAgg<
                     TPgDirectFunc<&array_agg_transfn>, false, TArgs_array_agg_transfn_Policy,
                     TPgDirectFunc<&array_agg_combine>, false, TArgs_array_agg_combine_Policy,
                     true, TPgDirectFunc<&array_agg_serialize>, TArgs_array_agg_serialize_Policy,
                     true, TPgDirectFunc<&array_agg_deserialize>, TArgs_array_agg_deserialize_Policy,
                     true, TPgDirectFunc<&array_agg_finalfn>, false, TArgs_array_agg_finalfn_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_bool_and_2517() {
    return TGenericAgg<
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bool_or_2518() {
    return TGenericAgg<
                     TPgDirectFunc<&boolor_statefunc>, true, TArgs_boolor_statefunc_Policy,
                     TPgDirectFunc<&boolor_statefunc>, true, TArgs_boolor_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_every_2519() {
    return TGenericAgg<
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_var_samp_2641() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_2642() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_2643() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_2644() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_samp_2645() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_samp_2646() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_2712() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_2713() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_2714() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_2715() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_samp_2716() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_samp_2717() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_2718() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_pop>, false, TArgs_numeric_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_2719() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_pop>, false, TArgs_numeric_poly_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_2720() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_pop>, false, TArgs_numeric_poly_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_2721() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_pop>, true, TArgs_float8_var_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_pop_2722() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_pop>, true, TArgs_float8_var_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_pop_2723() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_pop>, false, TArgs_numeric_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_2724() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_pop>, false, TArgs_numeric_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_2725() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_pop>, false, TArgs_numeric_poly_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_2726() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_pop>, false, TArgs_numeric_poly_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_2727() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_pop>, true, TArgs_float8_stddev_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_pop_2728() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_pop>, true, TArgs_float8_stddev_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_pop_2729() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_pop>, false, TArgs_numeric_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_max_2797() {
    return TGenericAgg<
                     TPgDirectFunc<&tidlarger>, true, TArgs_tidlarger_Policy,
                     TPgDirectFunc<&tidlarger>, true, TArgs_tidlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2798() {
    return TGenericAgg<
                     TPgDirectFunc<&tidsmaller>, true, TArgs_tidsmaller_Policy,
                     TPgDirectFunc<&tidsmaller>, true, TArgs_tidsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_2803() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc>, true, TArgs_int8inc_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_count_2818() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc_float8_float8>, true, TArgs_int8inc_float8_float8_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_sxx_2819() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_sxx>, true, TArgs_float8_regr_sxx_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_syy_2820() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_syy>, true, TArgs_float8_regr_syy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_sxy_2821() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_sxy>, true, TArgs_float8_regr_sxy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_avgx_2822() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_avgx>, true, TArgs_float8_regr_avgx_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_avgy_2823() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_avgy>, true, TArgs_float8_regr_avgy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_r2_2824() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_r2>, true, TArgs_float8_regr_r2_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_slope_2825() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_slope>, true, TArgs_float8_regr_slope_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_intercept_2826() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_intercept>, true, TArgs_float8_regr_intercept_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_covar_pop_2827() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_covar_pop>, true, TArgs_float8_covar_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_covar_samp_2828() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_covar_samp>, true, TArgs_float8_covar_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_corr_2829() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_corr>, true, TArgs_float8_corr_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_max_3526() {
    return TGenericAgg<
                     TPgDirectFunc<&enum_larger>, true, TArgs_enum_larger_Policy,
                     TPgDirectFunc<&enum_larger>, true, TArgs_enum_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_3527() {
    return TGenericAgg<
                     TPgDirectFunc<&enum_smaller>, true, TArgs_enum_smaller_Policy,
                     TPgDirectFunc<&enum_smaller>, true, TArgs_enum_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_string_agg_3538() {
    return TGenericAgg<
                     TPgDirectFunc<&string_agg_transfn>, false, TArgs_string_agg_transfn_Policy,
                     TPgDirectFunc<&string_agg_combine>, false, TArgs_string_agg_combine_Policy,
                     true, TPgDirectFunc<&string_agg_serialize>, TArgs_string_agg_serialize_Policy,
                     true, TPgDirectFunc<&string_agg_deserialize>, TArgs_string_agg_deserialize_Policy,
                     true, TPgDirectFunc<&string_agg_finalfn>, false, TArgs_string_agg_finalfn_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_string_agg_3545() {
    return TGenericAgg<
                     TPgDirectFunc<&bytea_string_agg_transfn>, false, TArgs_bytea_string_agg_transfn_Policy,
                     TPgDirectFunc<&string_agg_combine>, false, TArgs_string_agg_combine_Policy,
                     true, TPgDirectFunc<&string_agg_serialize>, TArgs_string_agg_serialize_Policy,
                     true, TPgDirectFunc<&string_agg_deserialize>, TArgs_string_agg_deserialize_Policy,
                     true, TPgDirectFunc<&bytea_string_agg_finalfn>, false, TArgs_bytea_string_agg_finalfn_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_max_3564() {
    return TGenericAgg<
                     TPgDirectFunc<&network_larger>, true, TArgs_network_larger_Policy,
                     TPgDirectFunc<&network_larger>, true, TArgs_network_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_3565() {
    return TGenericAgg<
                     TPgDirectFunc<&network_smaller>, true, TArgs_network_smaller_Policy,
                     TPgDirectFunc<&network_smaller>, true, TArgs_network_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_array_agg_4053() {
    return TGenericAgg<
                     TPgDirectFunc<&array_agg_array_transfn>, false, TArgs_array_agg_array_transfn_Policy,
                     TPgDirectFunc<&array_agg_array_combine>, false, TArgs_array_agg_array_combine_Policy,
                     true, TPgDirectFunc<&array_agg_array_serialize>, TArgs_array_agg_array_serialize_Policy,
                     true, TPgDirectFunc<&array_agg_array_deserialize>, TArgs_array_agg_array_deserialize_Policy,
                     true, TPgDirectFunc<&array_agg_array_finalfn>, false, TArgs_array_agg_array_finalfn_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_max_4189() {
    return TGenericAgg<
                     TPgDirectFunc<&pg_lsn_larger>, true, TArgs_pg_lsn_larger_Policy,
                     TPgDirectFunc<&pg_lsn_larger>, true, TArgs_pg_lsn_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_4190() {
    return TGenericAgg<
                     TPgDirectFunc<&pg_lsn_smaller>, true, TArgs_pg_lsn_smaller_Policy,
                     TPgDirectFunc<&pg_lsn_smaller>, true, TArgs_pg_lsn_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_4389() {
    return TGenericAgg<
                     TPgDirectFunc<&multirange_intersect_agg_transfn>, true, TArgs_multirange_intersect_agg_transfn_Policy,
                     TPgDirectFunc<&multirange_intersect_agg_transfn>, true, TArgs_multirange_intersect_agg_transfn_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_4450() {
    return TGenericAgg<
                     TPgDirectFunc<&range_intersect_agg_transfn>, true, TArgs_range_intersect_agg_transfn_Policy,
                     TPgDirectFunc<&range_intersect_agg_transfn>, true, TArgs_range_intersect_agg_transfn_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_5099() {
    return TGenericAgg<
                     TPgDirectFunc<&xid8_larger>, true, TArgs_xid8_larger_Policy,
                     TPgDirectFunc<&xid8_larger>, true, TArgs_xid8_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_5100() {
    return TGenericAgg<
                     TPgDirectFunc<&xid8_smaller>, true, TArgs_xid8_smaller_Policy,
                     TPgDirectFunc<&xid8_smaller>, true, TArgs_xid8_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6164() {
    return TGenericAgg<
                     TPgDirectFunc<&int2xor>, true, TArgs_int2xor_Policy,
                     TPgDirectFunc<&int2xor>, true, TArgs_int2xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6165() {
    return TGenericAgg<
                     TPgDirectFunc<&int4xor>, true, TArgs_int4xor_Policy,
                     TPgDirectFunc<&int4xor>, true, TArgs_int4xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6166() {
    return TGenericAgg<
                     TPgDirectFunc<&int8xor>, true, TArgs_int8xor_Policy,
                     TPgDirectFunc<&int8xor>, true, TArgs_int8xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6167() {
    return TGenericAgg<
                     TPgDirectFunc<&bitxor>, true, TArgs_bitxor_Policy,
                     TPgDirectFunc<&bitxor>, true, TArgs_bitxor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_any_value_6291() {
    return TGenericAgg<
                     TPgDirectFunc<&any_value_transfn>, true, TArgs_any_value_transfn_Policy,
                     TPgDirectFunc<&any_value_transfn>, true, TArgs_any_value_transfn_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
