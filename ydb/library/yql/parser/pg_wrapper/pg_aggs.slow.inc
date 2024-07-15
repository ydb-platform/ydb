auto MakePgAgg_max_2050() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&array_larger, &array_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2051() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&array_smaller, &array_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_avg_2100() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_avg_accum, &int8_avg_combine, &int8_avg_serialize, &int8_avg_deserialize, &numeric_poly_avg);
}
auto MakePgAgg_avg_2101() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&int4_avg_accum, &int4_avg_combine, nullptr, nullptr, &int8_avg);
}
auto MakePgAgg_avg_2102() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&int2_avg_accum, &int4_avg_combine, nullptr, nullptr, &int8_avg);
}
auto MakePgAgg_avg_2103() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_avg_accum, &numeric_avg_combine, &numeric_avg_serialize, &numeric_avg_deserialize, &numeric_avg);
}
auto MakePgAgg_avg_2104() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_avg);
}
auto MakePgAgg_avg_2105() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_avg);
}
auto MakePgAgg_avg_2106() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, false, true
                     >(&interval_accum, &interval_combine, nullptr, nullptr, &interval_avg);
}
auto MakePgAgg_sum_2107() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_avg_accum, &int8_avg_combine, &int8_avg_serialize, &int8_avg_deserialize, &numeric_poly_sum);
}
auto MakePgAgg_sum_2108() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4_sum, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2109() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2_sum, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2110() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4pl, &float4pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2111() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8pl, &float8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2112() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cash_pl, &cash_pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2113() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_pl, &interval_pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_sum_2114() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_avg_accum, &numeric_avg_combine, &numeric_avg_serialize, &numeric_avg_deserialize, &numeric_sum);
}
auto MakePgAgg_max_2115() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8larger, &int8larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2116() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4larger, &int4larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2117() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2larger, &int2larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2118() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&oidlarger, &oidlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2119() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4larger, &float4larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2120() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8larger, &float8larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2122() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&date_larger, &date_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2123() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&time_larger, &time_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2124() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&timetz_larger, &timetz_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2125() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cashlarger, &cashlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2126() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_larger, &timestamp_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2127() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_larger, &timestamp_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2128() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_larger, &interval_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2129() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&text_larger, &text_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2130() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&numeric_larger, &numeric_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2131() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8smaller, &int8smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2132() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4smaller, &int4smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2133() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2smaller, &int2smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2134() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&oidsmaller, &oidsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2135() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float4smaller, &float4smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2136() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&float8smaller, &float8smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2138() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&date_smaller, &date_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2139() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&time_smaller, &time_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2140() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&timetz_smaller, &timetz_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2141() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&cashsmaller, &cashsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2142() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_smaller, &timestamp_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2143() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&timestamp_smaller, &timestamp_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2144() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&interval_smaller, &interval_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2145() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&text_smaller, &text_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2146() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&numeric_smaller, &numeric_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_2147() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc_any, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_variance_2148() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_variance_2149() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_variance_2150() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_variance_2151() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_variance_2152() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_variance_2153() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_stddev_2154() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_stddev_2155() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_2156() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_2157() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_2158() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_2159() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_bit_and_2236() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2and, &int2and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2237() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2or, &int2or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2238() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4and, &int4and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2239() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4or, &int4or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2240() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8and, &int8and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2241() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8or, &int8or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_and_2242() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bit_and, &bit_and, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_or_2243() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bit_or, &bit_or, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_2244() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bpchar_larger, &bpchar_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2245() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bpchar_smaller, &bpchar_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_array_agg_2335() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&array_agg_transfn, &array_agg_combine, &array_agg_serialize, &array_agg_deserialize, &array_agg_finalfn);
}
auto MakePgAgg_bool_and_2517() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&booland_statefunc, &booland_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bool_or_2518() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&boolor_statefunc, &boolor_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_every_2519() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&booland_statefunc, &booland_statefunc, nullptr, nullptr, nullptr);
}
auto MakePgAgg_var_samp_2641() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_var_samp_2642() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_var_samp_2643() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_samp);
}
auto MakePgAgg_var_samp_2644() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_var_samp_2645() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_samp);
}
auto MakePgAgg_var_samp_2646() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_samp);
}
auto MakePgAgg_stddev_samp_2712() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_stddev_samp_2713() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_samp_2714() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_samp);
}
auto MakePgAgg_stddev_samp_2715() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_samp_2716() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_samp);
}
auto MakePgAgg_stddev_samp_2717() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_samp);
}
auto MakePgAgg_var_pop_2718() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_pop);
}
auto MakePgAgg_var_pop_2719() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_pop);
}
auto MakePgAgg_var_pop_2720() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_var_pop);
}
auto MakePgAgg_var_pop_2721() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_var_pop);
}
auto MakePgAgg_var_pop_2722() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_var_pop);
}
auto MakePgAgg_var_pop_2723() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_var_pop);
}
auto MakePgAgg_stddev_pop_2724() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int8_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_pop);
}
auto MakePgAgg_stddev_pop_2725() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int4_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_pop);
}
auto MakePgAgg_stddev_pop_2726() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&int2_accum, &numeric_poly_combine, &numeric_poly_serialize, &numeric_poly_deserialize, &numeric_poly_stddev_pop);
}
auto MakePgAgg_stddev_pop_2727() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float4_accum, &float8_combine, nullptr, nullptr, &float8_stddev_pop);
}
auto MakePgAgg_stddev_pop_2728() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_accum, &float8_combine, nullptr, nullptr, &float8_stddev_pop);
}
auto MakePgAgg_stddev_pop_2729() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&numeric_accum, &numeric_combine, &numeric_serialize, &numeric_deserialize, &numeric_stddev_pop);
}
auto MakePgAgg_max_2797() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&tidlarger, &tidlarger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_2798() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&tidsmaller, &tidsmaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_count_2803() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_count_2818() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, true
                     >(&int8inc_float8_float8, &int8pl, nullptr, nullptr, nullptr);
}
auto MakePgAgg_regr_sxx_2819() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_sxx);
}
auto MakePgAgg_regr_syy_2820() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_syy);
}
auto MakePgAgg_regr_sxy_2821() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_sxy);
}
auto MakePgAgg_regr_avgx_2822() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_avgx);
}
auto MakePgAgg_regr_avgy_2823() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_avgy);
}
auto MakePgAgg_regr_r2_2824() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_r2);
}
auto MakePgAgg_regr_slope_2825() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_slope);
}
auto MakePgAgg_regr_intercept_2826() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_regr_intercept);
}
auto MakePgAgg_covar_pop_2827() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_covar_pop);
}
auto MakePgAgg_covar_samp_2828() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_covar_samp);
}
auto MakePgAgg_corr_2829() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, false, true, true
                     >(&float8_regr_accum, &float8_regr_combine, nullptr, nullptr, &float8_corr);
}
auto MakePgAgg_max_3526() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&enum_larger, &enum_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_3527() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&enum_smaller, &enum_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_string_agg_3538() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&string_agg_transfn, &string_agg_combine, &string_agg_serialize, &string_agg_deserialize, &string_agg_finalfn);
}
auto MakePgAgg_string_agg_3545() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&bytea_string_agg_transfn, &string_agg_combine, &string_agg_serialize, &string_agg_deserialize, &bytea_string_agg_finalfn);
}
auto MakePgAgg_max_3564() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&network_larger, &network_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_3565() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&network_smaller, &network_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_array_agg_4053() {
    return TGenericAgg<
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, TDefaultArgsPolicy,
                     true, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, false, false, false
                     >(&array_agg_array_transfn, &array_agg_array_combine, &array_agg_array_serialize, &array_agg_array_deserialize, &array_agg_array_finalfn);
}
auto MakePgAgg_max_4189() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&pg_lsn_larger, &pg_lsn_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_4190() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&pg_lsn_smaller, &pg_lsn_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_4389() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&multirange_intersect_agg_transfn, &multirange_intersect_agg_transfn, nullptr, nullptr, nullptr);
}
auto MakePgAgg_range_intersect_agg_4450() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&range_intersect_agg_transfn, &range_intersect_agg_transfn, nullptr, nullptr, nullptr);
}
auto MakePgAgg_max_5099() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&xid8_larger, &xid8_larger, nullptr, nullptr, nullptr);
}
auto MakePgAgg_min_5100() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&xid8_smaller, &xid8_smaller, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6164() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int2xor, &int2xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6165() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int4xor, &int4xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6166() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&int8xor, &int8xor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_bit_xor_6167() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     false, false, false, false
                     >(&bitxor, &bitxor, nullptr, nullptr, nullptr);
}
auto MakePgAgg_any_value_6291() {
    return TGenericAgg<
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     TPgIndirectFunc, true, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, TDefaultArgsPolicy,
                     false, TPgIndirectFunc, false, TDefaultArgsPolicy,
                     true, true, true, false
                     >(&any_value_transfn, &any_value_transfn, nullptr, nullptr, nullptr);
}
