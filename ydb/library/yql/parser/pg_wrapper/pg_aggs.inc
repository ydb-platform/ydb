auto MakePgAgg_avg_1() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_avg_accum>, false, TArgs_int8_avg_accum_Policy,
                     TPgDirectFunc<&int8_avg_combine>, false, TArgs_int8_avg_combine_Policy,
                     true, TPgDirectFunc<&int8_avg_serialize>, TArgs_int8_avg_serialize_Policy,
                     true, TPgDirectFunc<&int8_avg_deserialize>, TArgs_int8_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_avg>, false, TArgs_numeric_poly_avg_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_avg_2() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_avg_accum>, true, TArgs_int4_avg_accum_Policy,
                     TPgDirectFunc<&int4_avg_combine>, true, TArgs_int4_avg_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&int8_avg>, true, TArgs_int8_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_3() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_avg_accum>, true, TArgs_int2_avg_accum_Policy,
                     TPgDirectFunc<&int4_avg_combine>, true, TArgs_int4_avg_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&int8_avg>, true, TArgs_int8_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_4() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_avg_accum>, false, TArgs_numeric_avg_accum_Policy,
                     TPgDirectFunc<&numeric_avg_combine>, false, TArgs_numeric_avg_combine_Policy,
                     true, TPgDirectFunc<&numeric_avg_serialize>, TArgs_numeric_avg_serialize_Policy,
                     true, TPgDirectFunc<&numeric_avg_deserialize>, TArgs_numeric_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_avg>, false, TArgs_numeric_avg_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_avg_5() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_avg>, true, TArgs_float8_avg_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_6() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_avg>, true, TArgs_float8_avg_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_avg_7() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_accum>, true, TArgs_interval_accum_Policy,
                     TPgDirectFunc<&interval_combine>, true, TArgs_interval_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&interval_avg>, true, TArgs_interval_avg_Policy,
                     false, false, false, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_sum_8() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_avg_accum>, false, TArgs_int8_avg_accum_Policy,
                     TPgDirectFunc<&int8_avg_combine>, false, TArgs_int8_avg_combine_Policy,
                     true, TPgDirectFunc<&int8_avg_serialize>, TArgs_int8_avg_serialize_Policy,
                     true, TPgDirectFunc<&int8_avg_deserialize>, TArgs_int8_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_sum>, false, TArgs_numeric_poly_sum_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_sum_9() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_sum>, false, TArgs_int4_sum_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_10() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_sum>, false, TArgs_int2_sum_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_11() {
    return TGenericAgg<
                     TPgDirectFunc<&float4pl>, true, TArgs_float4pl_Policy,
                     TPgDirectFunc<&float4pl>, true, TArgs_float4pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_12() {
    return TGenericAgg<
                     TPgDirectFunc<&float8pl>, true, TArgs_float8pl_Policy,
                     TPgDirectFunc<&float8pl>, true, TArgs_float8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_13() {
    return TGenericAgg<
                     TPgDirectFunc<&cash_pl>, true, TArgs_cash_pl_Policy,
                     TPgDirectFunc<&cash_pl>, true, TArgs_cash_pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_14() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_pl>, true, TArgs_interval_pl_Policy,
                     TPgDirectFunc<&interval_pl>, true, TArgs_interval_pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_15() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_avg_accum>, false, TArgs_numeric_avg_accum_Policy,
                     TPgDirectFunc<&numeric_avg_combine>, false, TArgs_numeric_avg_combine_Policy,
                     true, TPgDirectFunc<&numeric_avg_serialize>, TArgs_numeric_avg_serialize_Policy,
                     true, TPgDirectFunc<&numeric_avg_deserialize>, TArgs_numeric_avg_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_sum>, false, TArgs_numeric_sum_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_max_16() {
    return TGenericAgg<
                     TPgDirectFunc<&int8larger>, true, TArgs_int8larger_Policy,
                     TPgDirectFunc<&int8larger>, true, TArgs_int8larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_17() {
    return TGenericAgg<
                     TPgDirectFunc<&int4larger>, true, TArgs_int4larger_Policy,
                     TPgDirectFunc<&int4larger>, true, TArgs_int4larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_18() {
    return TGenericAgg<
                     TPgDirectFunc<&int2larger>, true, TArgs_int2larger_Policy,
                     TPgDirectFunc<&int2larger>, true, TArgs_int2larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_19() {
    return TGenericAgg<
                     TPgDirectFunc<&oidlarger>, true, TArgs_oidlarger_Policy,
                     TPgDirectFunc<&oidlarger>, true, TArgs_oidlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_20() {
    return TGenericAgg<
                     TPgDirectFunc<&float4larger>, true, TArgs_float4larger_Policy,
                     TPgDirectFunc<&float4larger>, true, TArgs_float4larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_21() {
    return TGenericAgg<
                     TPgDirectFunc<&float8larger>, true, TArgs_float8larger_Policy,
                     TPgDirectFunc<&float8larger>, true, TArgs_float8larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_22() {
    return TGenericAgg<
                     TPgDirectFunc<&date_larger>, true, TArgs_date_larger_Policy,
                     TPgDirectFunc<&date_larger>, true, TArgs_date_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_23() {
    return TGenericAgg<
                     TPgDirectFunc<&time_larger>, true, TArgs_time_larger_Policy,
                     TPgDirectFunc<&time_larger>, true, TArgs_time_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_24() {
    return TGenericAgg<
                     TPgDirectFunc<&timetz_larger>, true, TArgs_timetz_larger_Policy,
                     TPgDirectFunc<&timetz_larger>, true, TArgs_timetz_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_25() {
    return TGenericAgg<
                     TPgDirectFunc<&cashlarger>, true, TArgs_cashlarger_Policy,
                     TPgDirectFunc<&cashlarger>, true, TArgs_cashlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_26() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_27() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     TPgDirectFunc<&timestamp_larger>, true, TArgs_timestamp_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_28() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_larger>, true, TArgs_interval_larger_Policy,
                     TPgDirectFunc<&interval_larger>, true, TArgs_interval_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_29() {
    return TGenericAgg<
                     TPgDirectFunc<&text_larger>, true, TArgs_text_larger_Policy,
                     TPgDirectFunc<&text_larger>, true, TArgs_text_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_30() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_larger>, true, TArgs_numeric_larger_Policy,
                     TPgDirectFunc<&numeric_larger>, true, TArgs_numeric_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_31() {
    return TGenericAgg<
                     TPgDirectFunc<&array_larger>, true, TArgs_array_larger_Policy,
                     TPgDirectFunc<&array_larger>, true, TArgs_array_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_32() {
    return TGenericAgg<
                     TPgDirectFunc<&bpchar_larger>, true, TArgs_bpchar_larger_Policy,
                     TPgDirectFunc<&bpchar_larger>, true, TArgs_bpchar_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_33() {
    return TGenericAgg<
                     TPgDirectFunc<&tidlarger>, true, TArgs_tidlarger_Policy,
                     TPgDirectFunc<&tidlarger>, true, TArgs_tidlarger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_34() {
    return TGenericAgg<
                     TPgDirectFunc<&enum_larger>, true, TArgs_enum_larger_Policy,
                     TPgDirectFunc<&enum_larger>, true, TArgs_enum_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_35() {
    return TGenericAgg<
                     TPgDirectFunc<&network_larger>, true, TArgs_network_larger_Policy,
                     TPgDirectFunc<&network_larger>, true, TArgs_network_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_36() {
    return TGenericAgg<
                     TPgDirectFunc<&pg_lsn_larger>, true, TArgs_pg_lsn_larger_Policy,
                     TPgDirectFunc<&pg_lsn_larger>, true, TArgs_pg_lsn_larger_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_37() {
    return TGenericAgg<
                     TPgDirectFunc<&int8smaller>, true, TArgs_int8smaller_Policy,
                     TPgDirectFunc<&int8smaller>, true, TArgs_int8smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_38() {
    return TGenericAgg<
                     TPgDirectFunc<&int4smaller>, true, TArgs_int4smaller_Policy,
                     TPgDirectFunc<&int4smaller>, true, TArgs_int4smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_39() {
    return TGenericAgg<
                     TPgDirectFunc<&int2smaller>, true, TArgs_int2smaller_Policy,
                     TPgDirectFunc<&int2smaller>, true, TArgs_int2smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_40() {
    return TGenericAgg<
                     TPgDirectFunc<&oidsmaller>, true, TArgs_oidsmaller_Policy,
                     TPgDirectFunc<&oidsmaller>, true, TArgs_oidsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_41() {
    return TGenericAgg<
                     TPgDirectFunc<&float4smaller>, true, TArgs_float4smaller_Policy,
                     TPgDirectFunc<&float4smaller>, true, TArgs_float4smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_42() {
    return TGenericAgg<
                     TPgDirectFunc<&float8smaller>, true, TArgs_float8smaller_Policy,
                     TPgDirectFunc<&float8smaller>, true, TArgs_float8smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_43() {
    return TGenericAgg<
                     TPgDirectFunc<&date_smaller>, true, TArgs_date_smaller_Policy,
                     TPgDirectFunc<&date_smaller>, true, TArgs_date_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_44() {
    return TGenericAgg<
                     TPgDirectFunc<&time_smaller>, true, TArgs_time_smaller_Policy,
                     TPgDirectFunc<&time_smaller>, true, TArgs_time_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_45() {
    return TGenericAgg<
                     TPgDirectFunc<&timetz_smaller>, true, TArgs_timetz_smaller_Policy,
                     TPgDirectFunc<&timetz_smaller>, true, TArgs_timetz_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_46() {
    return TGenericAgg<
                     TPgDirectFunc<&cashsmaller>, true, TArgs_cashsmaller_Policy,
                     TPgDirectFunc<&cashsmaller>, true, TArgs_cashsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_47() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_48() {
    return TGenericAgg<
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     TPgDirectFunc<&timestamp_smaller>, true, TArgs_timestamp_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_49() {
    return TGenericAgg<
                     TPgDirectFunc<&interval_smaller>, true, TArgs_interval_smaller_Policy,
                     TPgDirectFunc<&interval_smaller>, true, TArgs_interval_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_50() {
    return TGenericAgg<
                     TPgDirectFunc<&text_smaller>, true, TArgs_text_smaller_Policy,
                     TPgDirectFunc<&text_smaller>, true, TArgs_text_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_51() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_smaller>, true, TArgs_numeric_smaller_Policy,
                     TPgDirectFunc<&numeric_smaller>, true, TArgs_numeric_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_52() {
    return TGenericAgg<
                     TPgDirectFunc<&array_smaller>, true, TArgs_array_smaller_Policy,
                     TPgDirectFunc<&array_smaller>, true, TArgs_array_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_53() {
    return TGenericAgg<
                     TPgDirectFunc<&bpchar_smaller>, true, TArgs_bpchar_smaller_Policy,
                     TPgDirectFunc<&bpchar_smaller>, true, TArgs_bpchar_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_54() {
    return TGenericAgg<
                     TPgDirectFunc<&tidsmaller>, true, TArgs_tidsmaller_Policy,
                     TPgDirectFunc<&tidsmaller>, true, TArgs_tidsmaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_55() {
    return TGenericAgg<
                     TPgDirectFunc<&enum_smaller>, true, TArgs_enum_smaller_Policy,
                     TPgDirectFunc<&enum_smaller>, true, TArgs_enum_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_56() {
    return TGenericAgg<
                     TPgDirectFunc<&network_smaller>, true, TArgs_network_smaller_Policy,
                     TPgDirectFunc<&network_smaller>, true, TArgs_network_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_57() {
    return TGenericAgg<
                     TPgDirectFunc<&pg_lsn_smaller>, true, TArgs_pg_lsn_smaller_Policy,
                     TPgDirectFunc<&pg_lsn_smaller>, true, TArgs_pg_lsn_smaller_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_58() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc_any>, true, TArgs_int8inc_any_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_59() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc>, true, TArgs_int8inc_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_var_pop_60() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_pop>, false, TArgs_numeric_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_61() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_pop>, false, TArgs_numeric_poly_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_62() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_pop>, false, TArgs_numeric_poly_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_pop_63() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_pop>, true, TArgs_float8_var_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_pop_64() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_pop>, true, TArgs_float8_var_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_pop_65() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_pop>, false, TArgs_numeric_var_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_66() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_67() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_68() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_var_samp_69() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_samp_70() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_var_samp_71() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_72() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_73() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_74() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_var_samp>, false, TArgs_numeric_poly_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_variance_75() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_variance_76() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_var_samp>, true, TArgs_float8_var_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_variance_77() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_var_samp>, false, TArgs_numeric_var_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_78() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_pop>, false, TArgs_numeric_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_79() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_pop>, false, TArgs_numeric_poly_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_80() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_pop>, false, TArgs_numeric_poly_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_pop_81() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_pop>, true, TArgs_float8_stddev_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_pop_82() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_pop>, true, TArgs_float8_stddev_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_pop_83() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_pop>, false, TArgs_numeric_stddev_pop_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_84() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_85() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_86() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_samp_87() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_samp_88() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_samp_89() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_90() {
    return TGenericAgg<
                     TPgDirectFunc<&int8_accum>, false, TArgs_int8_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_91() {
    return TGenericAgg<
                     TPgDirectFunc<&int4_accum>, false, TArgs_int4_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_92() {
    return TGenericAgg<
                     TPgDirectFunc<&int2_accum>, false, TArgs_int2_accum_Policy,
                     TPgDirectFunc<&numeric_poly_combine>, false, TArgs_numeric_poly_combine_Policy,
                     true, TPgDirectFunc<&numeric_poly_serialize>, TArgs_numeric_poly_serialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_deserialize>, TArgs_numeric_poly_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_poly_stddev_samp>, false, TArgs_numeric_poly_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_stddev_93() {
    return TGenericAgg<
                     TPgDirectFunc<&float4_accum>, true, TArgs_float4_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_94() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_accum>, true, TArgs_float8_accum_Policy,
                     TPgDirectFunc<&float8_combine>, true, TArgs_float8_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_stddev_samp>, true, TArgs_float8_stddev_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_stddev_95() {
    return TGenericAgg<
                     TPgDirectFunc<&numeric_accum>, false, TArgs_numeric_accum_Policy,
                     TPgDirectFunc<&numeric_combine>, false, TArgs_numeric_combine_Policy,
                     true, TPgDirectFunc<&numeric_serialize>, TArgs_numeric_serialize_Policy,
                     true, TPgDirectFunc<&numeric_deserialize>, TArgs_numeric_deserialize_Policy,
                     true, TPgDirectFunc<&numeric_stddev_samp>, false, TArgs_numeric_stddev_samp_Policy,
                     true, false, false, false
                     >({}, {}, {}, {}, {});
}
auto MakePgAgg_regr_count_96() {
    return TGenericAgg<
                     TPgDirectFunc<&int8inc_float8_float8>, true, TArgs_int8inc_float8_float8_Policy,
                     TPgDirectFunc<&int8pl>, true, TArgs_int8pl_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_sxx_97() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_sxx>, true, TArgs_float8_regr_sxx_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_syy_98() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_syy>, true, TArgs_float8_regr_syy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_sxy_99() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_sxy>, true, TArgs_float8_regr_sxy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_avgx_100() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_avgx>, true, TArgs_float8_regr_avgx_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_avgy_101() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_avgy>, true, TArgs_float8_regr_avgy_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_r2_102() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_r2>, true, TArgs_float8_regr_r2_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_slope_103() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_slope>, true, TArgs_float8_regr_slope_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_regr_intercept_104() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_regr_intercept>, true, TArgs_float8_regr_intercept_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_covar_pop_105() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_covar_pop>, true, TArgs_float8_covar_pop_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_covar_samp_106() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_covar_samp>, true, TArgs_float8_covar_samp_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_corr_107() {
    return TGenericAgg<
                     TPgDirectFunc<&float8_regr_accum>, true, TArgs_float8_regr_accum_Policy,
                     TPgDirectFunc<&float8_regr_combine>, true, TArgs_float8_regr_combine_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgDirectFunc<&float8_corr>, true, TArgs_float8_corr_Policy,
                     false, false, true, true
                     >({}, {}, nullptr, nullptr, {});
}
auto MakePgAgg_bool_and_108() {
    return TGenericAgg<
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bool_or_109() {
    return TGenericAgg<
                     TPgDirectFunc<&boolor_statefunc>, true, TArgs_boolor_statefunc_Policy,
                     TPgDirectFunc<&boolor_statefunc>, true, TArgs_boolor_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_every_110() {
    return TGenericAgg<
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     TPgDirectFunc<&booland_statefunc>, true, TArgs_booland_statefunc_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_111() {
    return TGenericAgg<
                     TPgDirectFunc<&int2and>, true, TArgs_int2and_Policy,
                     TPgDirectFunc<&int2and>, true, TArgs_int2and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_112() {
    return TGenericAgg<
                     TPgDirectFunc<&int2or>, true, TArgs_int2or_Policy,
                     TPgDirectFunc<&int2or>, true, TArgs_int2or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_113() {
    return TGenericAgg<
                     TPgDirectFunc<&int2xor>, true, TArgs_int2xor_Policy,
                     TPgDirectFunc<&int2xor>, true, TArgs_int2xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_114() {
    return TGenericAgg<
                     TPgDirectFunc<&int4and>, true, TArgs_int4and_Policy,
                     TPgDirectFunc<&int4and>, true, TArgs_int4and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_115() {
    return TGenericAgg<
                     TPgDirectFunc<&int4or>, true, TArgs_int4or_Policy,
                     TPgDirectFunc<&int4or>, true, TArgs_int4or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_116() {
    return TGenericAgg<
                     TPgDirectFunc<&int4xor>, true, TArgs_int4xor_Policy,
                     TPgDirectFunc<&int4xor>, true, TArgs_int4xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_117() {
    return TGenericAgg<
                     TPgDirectFunc<&int8and>, true, TArgs_int8and_Policy,
                     TPgDirectFunc<&int8and>, true, TArgs_int8and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_118() {
    return TGenericAgg<
                     TPgDirectFunc<&int8or>, true, TArgs_int8or_Policy,
                     TPgDirectFunc<&int8or>, true, TArgs_int8or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_119() {
    return TGenericAgg<
                     TPgDirectFunc<&int8xor>, true, TArgs_int8xor_Policy,
                     TPgDirectFunc<&int8xor>, true, TArgs_int8xor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_120() {
    return TGenericAgg<
                     TPgDirectFunc<&bit_and>, true, TArgs_bit_and_Policy,
                     TPgDirectFunc<&bit_and>, true, TArgs_bit_and_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_121() {
    return TGenericAgg<
                     TPgDirectFunc<&bit_or>, true, TArgs_bit_or_Policy,
                     TPgDirectFunc<&bit_or>, true, TArgs_bit_or_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_122() {
    return TGenericAgg<
                     TPgDirectFunc<&bitxor>, true, TArgs_bitxor_Policy,
                     TPgDirectFunc<&bitxor>, true, TArgs_bitxor_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_128() {
    return TGenericAgg<
                     TPgDirectFunc<&range_intersect_agg_transfn>, true, TArgs_range_intersect_agg_transfn_Policy,
                     TPgDirectFunc<&range_intersect_agg_transfn>, true, TArgs_range_intersect_agg_transfn_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_129() {
    return TGenericAgg<
                     TPgDirectFunc<&multirange_intersect_agg_transfn>, true, TArgs_multirange_intersect_agg_transfn_Policy,
                     TPgDirectFunc<&multirange_intersect_agg_transfn>, true, TArgs_multirange_intersect_agg_transfn_Policy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >({}, {}, nullptr, nullptr, nullptr);
}
