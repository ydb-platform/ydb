auto MakePgAgg_avg_1() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_avg_accum, &int8_avg_combine, &int8_avg_serialize, &int8_avg_deserialize, &numeric_poly_avg);
}
auto MakePgAgg_avg_2() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&int4_avg_accum, &int4_avg_combine, nullptr, nullptr, &int8_avg);
}
auto MakePgAgg_avg_3() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&int2_avg_accum, &int4_avg_combine, nullptr, nullptr, &int8_avg);
}
auto MakePgAgg_avg_4() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_avg_accum, &numeric_avg_combine, &numeric_avg_serialize, &numeric_avg_deserialize, &numeric_avg);
}
auto MakePgAgg_avg_5() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_avg);
}
auto MakePgAgg_avg_6() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_avg);
}
auto MakePgAgg_avg_7() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&interval_accum, &interval_combine, nullptr, nullptr, &interval_avg);
}
auto MakePgAgg_sum_8() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_avg_accum, &int8_avg_combine, &int8_avg_serialize, &int8_avg_deserialize, &numeric_poly_sum);
}
auto MakePgAgg_sum_9() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4_sum, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_10() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2_sum, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_11() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4pl, &float4pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_12() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8pl, &float8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_13() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cash_pl, &cash_pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_14() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_pl, &interval_pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_15() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_avg_accum, &numeric_avg_combine, &numeric_avg_serialize, &numeric_avg_deserialize, &numeric_sum);
}
auto MakePgAgg_max_16() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8larger, &int8larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_17() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4larger, &int4larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_18() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2larger, &int2larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_19() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&oidlarger, &oidlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_20() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4larger, &float4larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_21() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8larger, &float8larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_22() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&date_larger, &date_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_23() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&time_larger, &time_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_24() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&timetz_larger, &timetz_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_25() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cashlarger, &cashlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_26() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_larger, &timestamp_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_27() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_larger, &timestamp_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_28() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_larger, &interval_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_29() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&text_larger, &text_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_30() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&numeric_larger, &numeric_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_31() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&array_larger, &array_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_32() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bpchar_larger, &bpchar_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_33() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&tidlarger, &tidlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_34() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&enum_larger, &enum_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_35() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&network_larger, &network_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_36() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&pg_lsn_larger, &pg_lsn_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_37() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8smaller, &int8smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_38() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4smaller, &int4smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_39() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2smaller, &int2smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_40() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&oidsmaller, &oidsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_41() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4smaller, &float4smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_42() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8smaller, &float8smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_43() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&date_smaller, &date_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_44() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&time_smaller, &time_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_45() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&timetz_smaller, &timetz_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_46() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cashsmaller, &cashsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_47() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_smaller, &timestamp_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_48() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_smaller, &timestamp_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_49() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_smaller, &interval_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_50() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&text_smaller, &text_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_51() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&numeric_smaller, &numeric_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_52() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&array_smaller, &array_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_53() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bpchar_smaller, &bpchar_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_54() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&tidsmaller, &tidsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_55() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&enum_smaller, &enum_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_56() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&network_smaller, &network_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_57() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&pg_lsn_smaller, &pg_lsn_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_58() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc_any, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_59() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_var_pop_60() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_pop);
}
auto MakePgAgg_var_pop_61() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_pop);
}
auto MakePgAgg_var_pop_62() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_pop);
}
auto MakePgAgg_var_pop_63() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_pop);
}
auto MakePgAgg_var_pop_64() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_pop);
}
auto MakePgAgg_var_pop_65() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_pop);
}
auto MakePgAgg_var_samp_66() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_var_samp_67() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_var_samp_68() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_var_samp_69() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_var_samp_70() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_var_samp_71() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_variance_72() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_variance_73() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_variance_74() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_variance_75() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_variance_76() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_variance_77() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_stddev_pop_78() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_pop);
}
auto MakePgAgg_stddev_pop_79() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_pop);
}
auto MakePgAgg_stddev_pop_80() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_pop);
}
auto MakePgAgg_stddev_pop_81() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_pop);
}
auto MakePgAgg_stddev_pop_82() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_pop);
}
auto MakePgAgg_stddev_pop_83() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_pop);
}
auto MakePgAgg_stddev_samp_84() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_stddev_samp_85() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_samp_86() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_samp_87() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_samp_88() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_samp_89() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_stddev_90() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_stddev_91() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_92() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_93() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_94() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_95() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_regr_count_96() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc_float8_float8, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_sxx_97() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_sxx);
}
auto MakePgAgg_regr_syy_98() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_syy);
}
auto MakePgAgg_regr_sxy_99() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_sxy);
}
auto MakePgAgg_regr_avgx_100() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_avgx);
}
auto MakePgAgg_regr_avgy_101() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_avgy);
}
auto MakePgAgg_regr_r2_102() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_r2);
}
auto MakePgAgg_regr_slope_103() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_slope);
}
auto MakePgAgg_regr_intercept_104() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_intercept);
}
auto MakePgAgg_covar_pop_105() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_covar_pop);
}
auto MakePgAgg_covar_samp_106() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_covar_samp);
}
auto MakePgAgg_corr_107() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_corr);
}
auto MakePgAgg_bool_and_108() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&booland_statefunc, &booland_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bool_or_109() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&boolor_statefunc, &boolor_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_every_110() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&booland_statefunc, &booland_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_111() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2and, &int2and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_112() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2or, &int2or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_113() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2xor, &int2xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_114() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4and, &int4and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_115() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4or, &int4or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_116() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4xor, &int4xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_117() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8and, &int8and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_118() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8or, &int8or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_119() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8xor, &int8xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_120() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bit_and, &bit_and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_121() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bit_or, &bit_or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_122() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bitxor, &bitxor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_128() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&range_intersect_agg_transfn, &range_intersect_agg_transfn, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_129() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&multirange_intersect_agg_transfn, &multirange_intersect_agg_transfn, nullptr, nullptr, nullptr);
}
