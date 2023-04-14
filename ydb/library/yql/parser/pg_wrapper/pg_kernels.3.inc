struct TArgs_binary_decode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_binary_decode() { return &GenericExec<TPgDirectFunc<&binary_decode>, true, false, TArgs_binary_decode_Policy>; }
struct TArgs_binary_encode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_binary_encode() { return &GenericExec<TPgDirectFunc<&binary_encode>, true, false, TArgs_binary_encode_Policy>; }
struct TArgs_binary_upgrade_create_empty_extension_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,false,true,false,false,false,false};
};
TExecFunc arrow_binary_upgrade_create_empty_extension() { return &GenericExec<TPgDirectFunc<&binary_upgrade_create_empty_extension>, false, true, TArgs_binary_upgrade_create_empty_extension_Policy>; }
struct TArgs_binary_upgrade_set_missing_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_binary_upgrade_set_missing_value() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_missing_value>, true, true, TArgs_binary_upgrade_set_missing_value_Policy>; }
struct TArgs_binary_upgrade_set_next_array_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_array_pg_type_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_array_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_array_pg_type_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_heap_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_heap_pg_class_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_heap_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_heap_pg_class_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_index_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_index_pg_class_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_index_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_index_pg_class_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_multirange_array_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_multirange_array_pg_type_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_multirange_array_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_multirange_array_pg_type_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_multirange_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_multirange_pg_type_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_multirange_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_multirange_pg_type_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_pg_authid_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_authid_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_authid_oid>, true, true, TArgs_binary_upgrade_set_next_pg_authid_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_pg_enum_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_enum_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_enum_oid>, true, true, TArgs_binary_upgrade_set_next_pg_enum_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_type_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_pg_type_oid_Policy>; }
struct TArgs_binary_upgrade_set_next_toast_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_toast_pg_class_oid() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_next_toast_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_toast_pg_class_oid_Policy>; }
struct TArgs_binary_upgrade_set_record_init_privs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_record_init_privs() { return &GenericExec<TPgDirectFunc<&binary_upgrade_set_record_init_privs>, true, true, TArgs_binary_upgrade_set_record_init_privs_Policy>; }
struct TArgs_bool_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_bool_accum() { return &GenericExec<TPgDirectFunc<&bool_accum>, false, true, TArgs_bool_accum_Policy>; }
struct TArgs_bool_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_bool_accum_inv() { return &GenericExec<TPgDirectFunc<&bool_accum_inv>, false, true, TArgs_bool_accum_inv_Policy>; }
struct TArgs_bool_alltrue_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bool_alltrue() { return &GenericExec<TPgDirectFunc<&bool_alltrue>, true, true, TArgs_bool_alltrue_Policy>; }
struct TArgs_bool_anytrue_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bool_anytrue() { return &GenericExec<TPgDirectFunc<&bool_anytrue>, true, true, TArgs_bool_anytrue_Policy>; }
struct TArgs_bool_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bool_int4() { return &GenericExec<TPgDirectFunc<&bool_int4>, true, true, TArgs_bool_int4_Policy>; }
struct TArgs_booland_statefunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_booland_statefunc() { return &GenericExec<TPgDirectFunc<&booland_statefunc>, true, true, TArgs_booland_statefunc_Policy>; }
struct TArgs_booleq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_booleq() { return &GenericExec<TPgDirectFunc<&booleq>, true, true, TArgs_booleq_Policy>; }
struct TArgs_boolge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boolge() { return &GenericExec<TPgDirectFunc<&boolge>, true, true, TArgs_boolge_Policy>; }
struct TArgs_boolgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boolgt() { return &GenericExec<TPgDirectFunc<&boolgt>, true, true, TArgs_boolgt_Policy>; }
struct TArgs_boolin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_boolin() { return &GenericExec<TPgDirectFunc<&boolin>, true, true, TArgs_boolin_Policy>; }
struct TArgs_boolle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boolle() { return &GenericExec<TPgDirectFunc<&boolle>, true, true, TArgs_boolle_Policy>; }
struct TArgs_boollt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boollt() { return &GenericExec<TPgDirectFunc<&boollt>, true, true, TArgs_boollt_Policy>; }
struct TArgs_boolne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boolne() { return &GenericExec<TPgDirectFunc<&boolne>, true, true, TArgs_boolne_Policy>; }
struct TArgs_boolor_statefunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_boolor_statefunc() { return &GenericExec<TPgDirectFunc<&boolor_statefunc>, true, true, TArgs_boolor_statefunc_Policy>; }
struct TArgs_boolout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_boolout() { return &GenericExec<TPgDirectFunc<&boolout>, true, false, TArgs_boolout_Policy>; }
struct TArgs_boolrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_boolrecv() { return &GenericExec<TPgDirectFunc<&boolrecv>, true, true, TArgs_boolrecv_Policy>; }
struct TArgs_boolsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_boolsend() { return &GenericExec<TPgDirectFunc<&boolsend>, true, false, TArgs_boolsend_Policy>; }
struct TArgs_booltext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_booltext() { return &GenericExec<TPgDirectFunc<&booltext>, true, false, TArgs_booltext_Policy>; }
struct TArgs_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bpchar() { return &GenericExec<TPgDirectFunc<&bpchar>, true, false, TArgs_bpchar_Policy>; }
struct TArgs_bpchar_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_larger() { return &GenericExec<TPgDirectFunc<&bpchar_larger>, true, false, TArgs_bpchar_larger_Policy>; }
struct TArgs_bpchar_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpchar_name() { return &GenericExec<TPgDirectFunc<&bpchar_name>, true, false, TArgs_bpchar_name_Policy>; }
struct TArgs_bpchar_pattern_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_ge() { return &GenericExec<TPgDirectFunc<&bpchar_pattern_ge>, true, true, TArgs_bpchar_pattern_ge_Policy>; }
struct TArgs_bpchar_pattern_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_gt() { return &GenericExec<TPgDirectFunc<&bpchar_pattern_gt>, true, true, TArgs_bpchar_pattern_gt_Policy>; }
struct TArgs_bpchar_pattern_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_le() { return &GenericExec<TPgDirectFunc<&bpchar_pattern_le>, true, true, TArgs_bpchar_pattern_le_Policy>; }
struct TArgs_bpchar_pattern_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_lt() { return &GenericExec<TPgDirectFunc<&bpchar_pattern_lt>, true, true, TArgs_bpchar_pattern_lt_Policy>; }
struct TArgs_bpchar_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_smaller() { return &GenericExec<TPgDirectFunc<&bpchar_smaller>, true, false, TArgs_bpchar_smaller_Policy>; }
struct TArgs_bpchar_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bpchar_sortsupport() { return &GenericExec<TPgDirectFunc<&bpchar_sortsupport>, true, true, TArgs_bpchar_sortsupport_Policy>; }
struct TArgs_bpcharcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharcmp() { return &GenericExec<TPgDirectFunc<&bpcharcmp>, true, true, TArgs_bpcharcmp_Policy>; }
struct TArgs_bpchareq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchareq() { return &GenericExec<TPgDirectFunc<&bpchareq>, true, true, TArgs_bpchareq_Policy>; }
struct TArgs_bpcharge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharge() { return &GenericExec<TPgDirectFunc<&bpcharge>, true, true, TArgs_bpcharge_Policy>; }
struct TArgs_bpchargt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchargt() { return &GenericExec<TPgDirectFunc<&bpchargt>, true, true, TArgs_bpchargt_Policy>; }
struct TArgs_bpcharin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bpcharin() { return &GenericExec<TPgDirectFunc<&bpcharin>, true, false, TArgs_bpcharin_Policy>; }
struct TArgs_bpcharle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharle() { return &GenericExec<TPgDirectFunc<&bpcharle>, true, true, TArgs_bpcharle_Policy>; }
struct TArgs_bpcharlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharlen() { return &GenericExec<TPgDirectFunc<&bpcharlen>, true, true, TArgs_bpcharlen_Policy>; }
struct TArgs_bpcharlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharlt() { return &GenericExec<TPgDirectFunc<&bpcharlt>, true, true, TArgs_bpcharlt_Policy>; }
struct TArgs_bpcharne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharne() { return &GenericExec<TPgDirectFunc<&bpcharne>, true, true, TArgs_bpcharne_Policy>; }
struct TArgs_bpcharoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharoctetlen() { return &GenericExec<TPgDirectFunc<&bpcharoctetlen>, true, true, TArgs_bpcharoctetlen_Policy>; }
struct TArgs_bpcharout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharout() { return &GenericExec<TPgDirectFunc<&bpcharout>, true, false, TArgs_bpcharout_Policy>; }
struct TArgs_bpcharrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_bpcharrecv() { return &GenericExec<TPgDirectFunc<&bpcharrecv>, true, false, TArgs_bpcharrecv_Policy>; }
struct TArgs_bpcharsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharsend() { return &GenericExec<TPgDirectFunc<&bpcharsend>, true, false, TArgs_bpcharsend_Policy>; }
struct TArgs_bpchartypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpchartypmodin() { return &GenericExec<TPgDirectFunc<&bpchartypmodin>, true, true, TArgs_bpchartypmodin_Policy>; }
struct TArgs_bpchartypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bpchartypmodout() { return &GenericExec<TPgDirectFunc<&bpchartypmodout>, true, false, TArgs_bpchartypmodout_Policy>; }
struct TArgs_brin_bloom_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_bloom_add_value() { return &GenericExec<TPgDirectFunc<&brin_bloom_add_value>, true, true, TArgs_brin_bloom_add_value_Policy>; }
struct TArgs_brin_bloom_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_bloom_consistent() { return &GenericExec<TPgDirectFunc<&brin_bloom_consistent>, true, true, TArgs_brin_bloom_consistent_Policy>; }
struct TArgs_brin_bloom_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_bloom_opcinfo() { return &GenericExec<TPgDirectFunc<&brin_bloom_opcinfo>, true, true, TArgs_brin_bloom_opcinfo_Policy>; }
struct TArgs_brin_bloom_options_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_bloom_options() { return &GenericExec<TPgDirectFunc<&brin_bloom_options>, false, true, TArgs_brin_bloom_options_Policy>; }
struct TArgs_brin_bloom_summary_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_bloom_summary_in() { return &GenericExec<TPgDirectFunc<&brin_bloom_summary_in>, true, false, TArgs_brin_bloom_summary_in_Policy>; }
struct TArgs_brin_bloom_summary_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_bloom_summary_out() { return &GenericExec<TPgDirectFunc<&brin_bloom_summary_out>, true, false, TArgs_brin_bloom_summary_out_Policy>; }
struct TArgs_brin_bloom_summary_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_bloom_summary_recv() { return &GenericExec<TPgDirectFunc<&brin_bloom_summary_recv>, true, false, TArgs_brin_bloom_summary_recv_Policy>; }
struct TArgs_brin_bloom_summary_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_bloom_summary_send() { return &GenericExec<TPgDirectFunc<&brin_bloom_summary_send>, true, false, TArgs_brin_bloom_summary_send_Policy>; }
struct TArgs_brin_bloom_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_bloom_union() { return &GenericExec<TPgDirectFunc<&brin_bloom_union>, true, true, TArgs_brin_bloom_union_Policy>; }
struct TArgs_brin_minmax_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_minmax_add_value() { return &GenericExec<TPgDirectFunc<&brin_minmax_add_value>, true, true, TArgs_brin_minmax_add_value_Policy>; }
struct TArgs_brin_minmax_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_minmax_consistent() { return &GenericExec<TPgDirectFunc<&brin_minmax_consistent>, true, true, TArgs_brin_minmax_consistent_Policy>; }
struct TArgs_brin_minmax_multi_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_minmax_multi_add_value() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_add_value>, true, true, TArgs_brin_minmax_multi_add_value_Policy>; }
struct TArgs_brin_minmax_multi_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_minmax_multi_consistent() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_consistent>, true, true, TArgs_brin_minmax_multi_consistent_Policy>; }
struct TArgs_brin_minmax_multi_distance_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_date() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_date>, true, true, TArgs_brin_minmax_multi_distance_date_Policy>; }
struct TArgs_brin_minmax_multi_distance_float4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_float4() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_float4>, true, true, TArgs_brin_minmax_multi_distance_float4_Policy>; }
struct TArgs_brin_minmax_multi_distance_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_float8() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_float8>, true, true, TArgs_brin_minmax_multi_distance_float8_Policy>; }
struct TArgs_brin_minmax_multi_distance_inet_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_inet() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_inet>, true, true, TArgs_brin_minmax_multi_distance_inet_Policy>; }
struct TArgs_brin_minmax_multi_distance_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_int2() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_int2>, true, true, TArgs_brin_minmax_multi_distance_int2_Policy>; }
struct TArgs_brin_minmax_multi_distance_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_int4() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_int4>, true, true, TArgs_brin_minmax_multi_distance_int4_Policy>; }
struct TArgs_brin_minmax_multi_distance_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_int8() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_int8>, true, true, TArgs_brin_minmax_multi_distance_int8_Policy>; }
struct TArgs_brin_minmax_multi_distance_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_interval() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_interval>, true, true, TArgs_brin_minmax_multi_distance_interval_Policy>; }
struct TArgs_brin_minmax_multi_distance_macaddr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_macaddr() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_macaddr>, true, true, TArgs_brin_minmax_multi_distance_macaddr_Policy>; }
struct TArgs_brin_minmax_multi_distance_macaddr8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_macaddr8() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_macaddr8>, true, true, TArgs_brin_minmax_multi_distance_macaddr8_Policy>; }
struct TArgs_brin_minmax_multi_distance_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_numeric() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_numeric>, true, true, TArgs_brin_minmax_multi_distance_numeric_Policy>; }
struct TArgs_brin_minmax_multi_distance_pg_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_pg_lsn() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_pg_lsn>, true, true, TArgs_brin_minmax_multi_distance_pg_lsn_Policy>; }
struct TArgs_brin_minmax_multi_distance_tid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_tid() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_tid>, true, true, TArgs_brin_minmax_multi_distance_tid_Policy>; }
struct TArgs_brin_minmax_multi_distance_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_time() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_time>, true, true, TArgs_brin_minmax_multi_distance_time_Policy>; }
struct TArgs_brin_minmax_multi_distance_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_timestamp() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_timestamp>, true, true, TArgs_brin_minmax_multi_distance_timestamp_Policy>; }
struct TArgs_brin_minmax_multi_distance_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_timetz() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_timetz>, true, true, TArgs_brin_minmax_multi_distance_timetz_Policy>; }
struct TArgs_brin_minmax_multi_distance_uuid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_minmax_multi_distance_uuid() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_distance_uuid>, true, true, TArgs_brin_minmax_multi_distance_uuid_Policy>; }
struct TArgs_brin_minmax_multi_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_minmax_multi_opcinfo() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_opcinfo>, true, true, TArgs_brin_minmax_multi_opcinfo_Policy>; }
struct TArgs_brin_minmax_multi_options_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_minmax_multi_options() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_options>, false, true, TArgs_brin_minmax_multi_options_Policy>; }
struct TArgs_brin_minmax_multi_summary_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_minmax_multi_summary_in() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_summary_in>, true, false, TArgs_brin_minmax_multi_summary_in_Policy>; }
struct TArgs_brin_minmax_multi_summary_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_minmax_multi_summary_out() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_summary_out>, true, false, TArgs_brin_minmax_multi_summary_out_Policy>; }
struct TArgs_brin_minmax_multi_summary_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_minmax_multi_summary_recv() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_summary_recv>, true, false, TArgs_brin_minmax_multi_summary_recv_Policy>; }
struct TArgs_brin_minmax_multi_summary_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_brin_minmax_multi_summary_send() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_summary_send>, true, false, TArgs_brin_minmax_multi_summary_send_Policy>; }
struct TArgs_brin_minmax_multi_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_minmax_multi_union() { return &GenericExec<TPgDirectFunc<&brin_minmax_multi_union>, true, true, TArgs_brin_minmax_multi_union_Policy>; }
struct TArgs_brin_minmax_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_minmax_opcinfo() { return &GenericExec<TPgDirectFunc<&brin_minmax_opcinfo>, true, true, TArgs_brin_minmax_opcinfo_Policy>; }
struct TArgs_brin_minmax_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_minmax_union() { return &GenericExec<TPgDirectFunc<&brin_minmax_union>, true, true, TArgs_brin_minmax_union_Policy>; }
struct TArgs_btbpchar_pattern_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btbpchar_pattern_cmp() { return &GenericExec<TPgDirectFunc<&btbpchar_pattern_cmp>, true, true, TArgs_btbpchar_pattern_cmp_Policy>; }
struct TArgs_btbpchar_pattern_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btbpchar_pattern_sortsupport() { return &GenericExec<TPgDirectFunc<&btbpchar_pattern_sortsupport>, true, true, TArgs_btbpchar_pattern_sortsupport_Policy>; }
struct TArgs_btequalimage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btequalimage() { return &GenericExec<TPgDirectFunc<&btequalimage>, true, true, TArgs_btequalimage_Policy>; }
struct TArgs_bthandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bthandler() { return &GenericExec<TPgDirectFunc<&bthandler>, true, true, TArgs_bthandler_Policy>; }
struct TArgs_bttidcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttidcmp() { return &GenericExec<TPgDirectFunc<&bttidcmp>, true, true, TArgs_bttidcmp_Policy>; }
struct TArgs_char_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_char_bpchar() { return &GenericExec<TPgDirectFunc<&char_bpchar>, true, false, TArgs_char_bpchar_Policy>; }
struct TArgs_char_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_char_text() { return &GenericExec<TPgDirectFunc<&char_text>, true, false, TArgs_char_text_Policy>; }
struct TArgs_chareq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_chareq() { return &GenericExec<TPgDirectFunc<&chareq>, true, true, TArgs_chareq_Policy>; }
struct TArgs_charge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_charge() { return &GenericExec<TPgDirectFunc<&charge>, true, true, TArgs_charge_Policy>; }
struct TArgs_chargt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_chargt() { return &GenericExec<TPgDirectFunc<&chargt>, true, true, TArgs_chargt_Policy>; }
struct TArgs_charin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_charin() { return &GenericExec<TPgDirectFunc<&charin>, true, true, TArgs_charin_Policy>; }
struct TArgs_charle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_charle() { return &GenericExec<TPgDirectFunc<&charle>, true, true, TArgs_charle_Policy>; }
struct TArgs_charlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_charlt() { return &GenericExec<TPgDirectFunc<&charlt>, true, true, TArgs_charlt_Policy>; }
struct TArgs_charne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_charne() { return &GenericExec<TPgDirectFunc<&charne>, true, true, TArgs_charne_Policy>; }
struct TArgs_charout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_charout() { return &GenericExec<TPgDirectFunc<&charout>, true, false, TArgs_charout_Policy>; }
struct TArgs_charrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_charrecv() { return &GenericExec<TPgDirectFunc<&charrecv>, true, true, TArgs_charrecv_Policy>; }
struct TArgs_charsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_charsend() { return &GenericExec<TPgDirectFunc<&charsend>, true, false, TArgs_charsend_Policy>; }
struct TArgs_chartoi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_chartoi4() { return &GenericExec<TPgDirectFunc<&chartoi4>, true, true, TArgs_chartoi4_Policy>; }
struct TArgs_cideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cideq() { return &GenericExec<TPgDirectFunc<&cideq>, true, true, TArgs_cideq_Policy>; }
struct TArgs_cidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidin() { return &GenericExec<TPgDirectFunc<&cidin>, true, true, TArgs_cidin_Policy>; }
struct TArgs_cidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidout() { return &GenericExec<TPgDirectFunc<&cidout>, true, false, TArgs_cidout_Policy>; }
struct TArgs_cidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidrecv() { return &GenericExec<TPgDirectFunc<&cidrecv>, true, true, TArgs_cidrecv_Policy>; }
struct TArgs_cidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidsend() { return &GenericExec<TPgDirectFunc<&cidsend>, true, false, TArgs_cidsend_Policy>; }
struct TArgs_current_database_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_database() { return &GenericExec<TPgDirectFunc<&current_database>, true, false, TArgs_current_database_Policy>; }
struct TArgs_current_query_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_query() { return &GenericExec<TPgDirectFunc<&current_query>, false, false, TArgs_current_query_Policy>; }
struct TArgs_currtid_byrelname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_currtid_byrelname() { return &GenericExec<TPgDirectFunc<&currtid_byrelname>, true, false, TArgs_currtid_byrelname_Policy>; }
struct TArgs_cursor_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,true,true,true,false};
};
TExecFunc arrow_cursor_to_xml() { return &GenericExec<TPgDirectFunc<&cursor_to_xml>, true, false, TArgs_cursor_to_xml_Policy>; }
struct TArgs_cursor_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_cursor_to_xmlschema() { return &GenericExec<TPgDirectFunc<&cursor_to_xmlschema>, true, false, TArgs_cursor_to_xmlschema_Policy>; }
struct TArgs_database_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xml() { return &GenericExec<TPgDirectFunc<&database_to_xml>, true, false, TArgs_database_to_xml_Policy>; }
struct TArgs_database_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xml_and_xmlschema() { return &GenericExec<TPgDirectFunc<&database_to_xml_and_xmlschema>, true, false, TArgs_database_to_xml_and_xmlschema_Policy>; }
struct TArgs_database_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xmlschema() { return &GenericExec<TPgDirectFunc<&database_to_xmlschema>, true, false, TArgs_database_to_xmlschema_Policy>; }
struct TArgs_date_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp() { return &GenericExec<TPgDirectFunc<&date_cmp>, true, true, TArgs_date_cmp_Policy>; }
struct TArgs_date_cmp_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp_timestamp() { return &GenericExec<TPgDirectFunc<&date_cmp_timestamp>, true, true, TArgs_date_cmp_timestamp_Policy>; }
struct TArgs_date_cmp_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp_timestamptz() { return &GenericExec<TPgDirectFunc<&date_cmp_timestamptz>, true, true, TArgs_date_cmp_timestamptz_Policy>; }
struct TArgs_date_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq() { return &GenericExec<TPgDirectFunc<&date_eq>, true, true, TArgs_date_eq_Policy>; }
struct TArgs_date_eq_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq_timestamp() { return &GenericExec<TPgDirectFunc<&date_eq_timestamp>, true, true, TArgs_date_eq_timestamp_Policy>; }
struct TArgs_date_eq_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq_timestamptz() { return &GenericExec<TPgDirectFunc<&date_eq_timestamptz>, true, true, TArgs_date_eq_timestamptz_Policy>; }
struct TArgs_date_finite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_finite() { return &GenericExec<TPgDirectFunc<&date_finite>, true, true, TArgs_date_finite_Policy>; }
struct TArgs_date_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge() { return &GenericExec<TPgDirectFunc<&date_ge>, true, true, TArgs_date_ge_Policy>; }
struct TArgs_date_ge_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge_timestamp() { return &GenericExec<TPgDirectFunc<&date_ge_timestamp>, true, true, TArgs_date_ge_timestamp_Policy>; }
struct TArgs_date_ge_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge_timestamptz() { return &GenericExec<TPgDirectFunc<&date_ge_timestamptz>, true, true, TArgs_date_ge_timestamptz_Policy>; }
struct TArgs_date_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt() { return &GenericExec<TPgDirectFunc<&date_gt>, true, true, TArgs_date_gt_Policy>; }
struct TArgs_date_gt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt_timestamp() { return &GenericExec<TPgDirectFunc<&date_gt_timestamp>, true, true, TArgs_date_gt_timestamp_Policy>; }
struct TArgs_date_gt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt_timestamptz() { return &GenericExec<TPgDirectFunc<&date_gt_timestamptz>, true, true, TArgs_date_gt_timestamptz_Policy>; }
struct TArgs_date_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_date_in() { return &GenericExec<TPgDirectFunc<&date_in>, true, true, TArgs_date_in_Policy>; }
struct TArgs_date_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_larger() { return &GenericExec<TPgDirectFunc<&date_larger>, true, true, TArgs_date_larger_Policy>; }
struct TArgs_date_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le() { return &GenericExec<TPgDirectFunc<&date_le>, true, true, TArgs_date_le_Policy>; }
struct TArgs_date_le_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le_timestamp() { return &GenericExec<TPgDirectFunc<&date_le_timestamp>, true, true, TArgs_date_le_timestamp_Policy>; }
struct TArgs_date_le_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le_timestamptz() { return &GenericExec<TPgDirectFunc<&date_le_timestamptz>, true, true, TArgs_date_le_timestamptz_Policy>; }
struct TArgs_date_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt() { return &GenericExec<TPgDirectFunc<&date_lt>, true, true, TArgs_date_lt_Policy>; }
struct TArgs_date_lt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt_timestamp() { return &GenericExec<TPgDirectFunc<&date_lt_timestamp>, true, true, TArgs_date_lt_timestamp_Policy>; }
struct TArgs_date_lt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt_timestamptz() { return &GenericExec<TPgDirectFunc<&date_lt_timestamptz>, true, true, TArgs_date_lt_timestamptz_Policy>; }
struct TArgs_date_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_mi() { return &GenericExec<TPgDirectFunc<&date_mi>, true, true, TArgs_date_mi_Policy>; }
struct TArgs_date_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_date_mi_interval() { return &GenericExec<TPgDirectFunc<&date_mi_interval>, true, true, TArgs_date_mi_interval_Policy>; }
struct TArgs_date_mii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_mii() { return &GenericExec<TPgDirectFunc<&date_mii>, true, true, TArgs_date_mii_Policy>; }
struct TArgs_date_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne() { return &GenericExec<TPgDirectFunc<&date_ne>, true, true, TArgs_date_ne_Policy>; }
struct TArgs_date_ne_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne_timestamp() { return &GenericExec<TPgDirectFunc<&date_ne_timestamp>, true, true, TArgs_date_ne_timestamp_Policy>; }
struct TArgs_date_ne_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne_timestamptz() { return &GenericExec<TPgDirectFunc<&date_ne_timestamptz>, true, true, TArgs_date_ne_timestamptz_Policy>; }
struct TArgs_date_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_out() { return &GenericExec<TPgDirectFunc<&date_out>, true, false, TArgs_date_out_Policy>; }
struct TArgs_date_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_date_pl_interval() { return &GenericExec<TPgDirectFunc<&date_pl_interval>, true, true, TArgs_date_pl_interval_Policy>; }
struct TArgs_date_pli_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_pli() { return &GenericExec<TPgDirectFunc<&date_pli>, true, true, TArgs_date_pli_Policy>; }
struct TArgs_date_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_recv() { return &GenericExec<TPgDirectFunc<&date_recv>, true, true, TArgs_date_recv_Policy>; }
struct TArgs_date_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_send() { return &GenericExec<TPgDirectFunc<&date_send>, true, false, TArgs_date_send_Policy>; }
struct TArgs_date_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_smaller() { return &GenericExec<TPgDirectFunc<&date_smaller>, true, true, TArgs_date_smaller_Policy>; }
struct TArgs_date_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_sortsupport() { return &GenericExec<TPgDirectFunc<&date_sortsupport>, true, true, TArgs_date_sortsupport_Policy>; }
struct TArgs_date_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_timestamp() { return &GenericExec<TPgDirectFunc<&date_timestamp>, true, true, TArgs_date_timestamp_Policy>; }
struct TArgs_date_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_timestamptz() { return &GenericExec<TPgDirectFunc<&date_timestamptz>, true, true, TArgs_date_timestamptz_Policy>; }
struct TArgs_daterange_canonical_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_daterange_canonical() { return &GenericExec<TPgDirectFunc<&daterange_canonical>, true, false, TArgs_daterange_canonical_Policy>; }
struct TArgs_daterange_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_daterange_subdiff() { return &GenericExec<TPgDirectFunc<&daterange_subdiff>, true, true, TArgs_daterange_subdiff_Policy>; }
struct TArgs_datetime_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datetime_timestamp() { return &GenericExec<TPgDirectFunc<&datetime_timestamp>, true, true, TArgs_datetime_timestamp_Policy>; }
struct TArgs_datetimetz_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_datetimetz_timestamptz() { return &GenericExec<TPgDirectFunc<&datetimetz_timestamptz>, true, true, TArgs_datetimetz_timestamptz_Policy>; }
struct TArgs_dispell_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dispell_init() { return &GenericExec<TPgDirectFunc<&dispell_init>, true, true, TArgs_dispell_init_Policy>; }
struct TArgs_dispell_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dispell_lexize() { return &GenericExec<TPgDirectFunc<&dispell_lexize>, true, true, TArgs_dispell_lexize_Policy>; }
struct TArgs_domain_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_domain_in() { return &GenericExec<TPgDirectFunc<&domain_in>, false, true, TArgs_domain_in_Policy>; }
struct TArgs_domain_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_domain_recv() { return &GenericExec<TPgDirectFunc<&domain_recv>, false, true, TArgs_domain_recv_Policy>; }
struct TArgs_elem_contained_by_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_elem_contained_by_range() { return &GenericExec<TPgDirectFunc<&elem_contained_by_range>, true, true, TArgs_elem_contained_by_range_Policy>; }
struct TArgs_extract_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_date() { return &GenericExec<TPgDirectFunc<&extract_date>, true, false, TArgs_extract_date_Policy>; }
struct TArgs_extract_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_time() { return &GenericExec<TPgDirectFunc<&extract_time>, true, false, TArgs_extract_time_Policy>; }
struct TArgs_extract_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_extract_timetz() { return &GenericExec<TPgDirectFunc<&extract_timetz>, true, false, TArgs_extract_timetz_Policy>; }
struct TArgs_generate_series_int4_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int4_support() { return &GenericExec<TPgDirectFunc<&generate_series_int4_support>, true, true, TArgs_generate_series_int4_support_Policy>; }
struct TArgs_gin_cmp_prefix_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_gin_cmp_prefix() { return &GenericExec<TPgDirectFunc<&gin_cmp_prefix>, true, true, TArgs_gin_cmp_prefix_Policy>; }
struct TArgs_gin_cmp_tslexeme_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_gin_cmp_tslexeme() { return &GenericExec<TPgDirectFunc<&gin_cmp_tslexeme>, true, true, TArgs_gin_cmp_tslexeme_Policy>; }
struct TArgs_gin_extract_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery() { return &GenericExec<TPgDirectFunc<&gin_extract_tsquery>, true, true, TArgs_gin_extract_tsquery_Policy>; }
struct TArgs_gin_extract_tsquery_5args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery_5args() { return &GenericExec<TPgDirectFunc<&gin_extract_tsquery_5args>, true, true, TArgs_gin_extract_tsquery_5args_Policy>; }
struct TArgs_gin_extract_tsquery_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery_oldsig() { return &GenericExec<TPgDirectFunc<&gin_extract_tsquery_oldsig>, true, true, TArgs_gin_extract_tsquery_oldsig_Policy>; }
struct TArgs_gin_extract_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_tsvector() { return &GenericExec<TPgDirectFunc<&gin_extract_tsvector>, true, true, TArgs_gin_extract_tsvector_Policy>; }
struct TArgs_gin_extract_tsvector_2args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_gin_extract_tsvector_2args() { return &GenericExec<TPgDirectFunc<&gin_extract_tsvector_2args>, true, true, TArgs_gin_extract_tsvector_2args_Policy>; }
struct TArgs_gin_tsquery_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent() { return &GenericExec<TPgDirectFunc<&gin_tsquery_consistent>, true, true, TArgs_gin_tsquery_consistent_Policy>; }
struct TArgs_gin_tsquery_consistent_6args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 6> IsFixedArg = {true,true,false,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent_6args() { return &GenericExec<TPgDirectFunc<&gin_tsquery_consistent_6args>, true, true, TArgs_gin_tsquery_consistent_6args_Policy>; }
struct TArgs_gin_tsquery_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent_oldsig() { return &GenericExec<TPgDirectFunc<&gin_tsquery_consistent_oldsig>, true, true, TArgs_gin_tsquery_consistent_oldsig_Policy>; }
struct TArgs_gin_tsquery_triconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_triconsistent() { return &GenericExec<TPgDirectFunc<&gin_tsquery_triconsistent>, true, true, TArgs_gin_tsquery_triconsistent_Policy>; }
struct TArgs_ginhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ginhandler() { return &GenericExec<TPgDirectFunc<&ginhandler>, true, true, TArgs_ginhandler_Policy>; }
struct TArgs_hash_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_range() { return &GenericExec<TPgDirectFunc<&hash_range>, true, true, TArgs_hash_range_Policy>; }
struct TArgs_hash_range_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_range_extended() { return &GenericExec<TPgDirectFunc<&hash_range_extended>, true, true, TArgs_hash_range_extended_Policy>; }
struct TArgs_hashbpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashbpchar() { return &GenericExec<TPgDirectFunc<&hashbpchar>, true, true, TArgs_hashbpchar_Policy>; }
struct TArgs_hashbpcharextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashbpcharextended() { return &GenericExec<TPgDirectFunc<&hashbpcharextended>, true, true, TArgs_hashbpcharextended_Policy>; }
struct TArgs_hashhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashhandler() { return &GenericExec<TPgDirectFunc<&hashhandler>, true, true, TArgs_hashhandler_Policy>; }
struct TArgs_hashtid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashtid() { return &GenericExec<TPgDirectFunc<&hashtid>, true, true, TArgs_hashtid_Policy>; }
struct TArgs_hashtidextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashtidextended() { return &GenericExec<TPgDirectFunc<&hashtidextended>, true, true, TArgs_hashtidextended_Policy>; }
struct TArgs_i2toi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2toi4() { return &GenericExec<TPgDirectFunc<&i2toi4>, true, true, TArgs_i2toi4_Policy>; }
struct TArgs_i4tochar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tochar() { return &GenericExec<TPgDirectFunc<&i4tochar>, true, true, TArgs_i4tochar_Policy>; }
struct TArgs_i4toi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4toi2() { return &GenericExec<TPgDirectFunc<&i4toi2>, true, true, TArgs_i4toi2_Policy>; }
struct TArgs_in_range_date_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_date_interval() { return &GenericExec<TPgDirectFunc<&in_range_date_interval>, true, true, TArgs_in_range_date_interval_Policy>; }
struct TArgs_in_range_int2_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int2() { return &GenericExec<TPgDirectFunc<&in_range_int2_int2>, true, true, TArgs_in_range_int2_int2_Policy>; }
struct TArgs_in_range_int2_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int4() { return &GenericExec<TPgDirectFunc<&in_range_int2_int4>, true, true, TArgs_in_range_int2_int4_Policy>; }
struct TArgs_in_range_int2_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int8() { return &GenericExec<TPgDirectFunc<&in_range_int2_int8>, true, true, TArgs_in_range_int2_int8_Policy>; }
struct TArgs_in_range_int4_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int2() { return &GenericExec<TPgDirectFunc<&in_range_int4_int2>, true, true, TArgs_in_range_int4_int2_Policy>; }
struct TArgs_in_range_int4_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int4() { return &GenericExec<TPgDirectFunc<&in_range_int4_int4>, true, true, TArgs_in_range_int4_int4_Policy>; }
struct TArgs_in_range_int4_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int8() { return &GenericExec<TPgDirectFunc<&in_range_int4_int8>, true, true, TArgs_in_range_int4_int8_Policy>; }
struct TArgs_in_range_time_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_time_interval() { return &GenericExec<TPgDirectFunc<&in_range_time_interval>, true, true, TArgs_in_range_time_interval_Policy>; }
struct TArgs_in_range_timetz_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_timetz_interval() { return &GenericExec<TPgDirectFunc<&in_range_timetz_interval>, true, true, TArgs_in_range_timetz_interval_Policy>; }
struct TArgs_inet_gist_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_inet_gist_compress() { return &GenericExec<TPgDirectFunc<&inet_gist_compress>, true, true, TArgs_inet_gist_compress_Policy>; }
struct TArgs_inet_gist_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_inet_gist_consistent() { return &GenericExec<TPgDirectFunc<&inet_gist_consistent>, true, true, TArgs_inet_gist_consistent_Policy>; }
struct TArgs_inet_gist_fetch_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_inet_gist_fetch() { return &GenericExec<TPgDirectFunc<&inet_gist_fetch>, true, true, TArgs_inet_gist_fetch_Policy>; }
struct TArgs_inet_gist_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_inet_gist_penalty() { return &GenericExec<TPgDirectFunc<&inet_gist_penalty>, true, true, TArgs_inet_gist_penalty_Policy>; }
struct TArgs_inet_gist_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_gist_picksplit() { return &GenericExec<TPgDirectFunc<&inet_gist_picksplit>, true, true, TArgs_inet_gist_picksplit_Policy>; }
struct TArgs_inet_gist_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_inet_gist_same() { return &GenericExec<TPgDirectFunc<&inet_gist_same>, true, true, TArgs_inet_gist_same_Policy>; }
struct TArgs_inet_gist_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_gist_union() { return &GenericExec<TPgDirectFunc<&inet_gist_union>, true, false, TArgs_inet_gist_union_Policy>; }
struct TArgs_int24div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24div() { return &GenericExec<TPgDirectFunc<&int24div>, true, true, TArgs_int24div_Policy>; }
struct TArgs_int24eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24eq() { return &GenericExec<TPgDirectFunc<&int24eq>, true, true, TArgs_int24eq_Policy>; }
struct TArgs_int24ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24ge() { return &GenericExec<TPgDirectFunc<&int24ge>, true, true, TArgs_int24ge_Policy>; }
struct TArgs_int24gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24gt() { return &GenericExec<TPgDirectFunc<&int24gt>, true, true, TArgs_int24gt_Policy>; }
struct TArgs_int24le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24le() { return &GenericExec<TPgDirectFunc<&int24le>, true, true, TArgs_int24le_Policy>; }
struct TArgs_int24lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24lt() { return &GenericExec<TPgDirectFunc<&int24lt>, true, true, TArgs_int24lt_Policy>; }
struct TArgs_int24mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24mi() { return &GenericExec<TPgDirectFunc<&int24mi>, true, true, TArgs_int24mi_Policy>; }
struct TArgs_int24mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24mul() { return &GenericExec<TPgDirectFunc<&int24mul>, true, true, TArgs_int24mul_Policy>; }
struct TArgs_int24ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24ne() { return &GenericExec<TPgDirectFunc<&int24ne>, true, true, TArgs_int24ne_Policy>; }
struct TArgs_int24pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24pl() { return &GenericExec<TPgDirectFunc<&int24pl>, true, true, TArgs_int24pl_Policy>; }
struct TArgs_int2abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2abs() { return &GenericExec<TPgDirectFunc<&int2abs>, true, true, TArgs_int2abs_Policy>; }
struct TArgs_int2and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2and() { return &GenericExec<TPgDirectFunc<&int2and>, true, true, TArgs_int2and_Policy>; }
struct TArgs_int2div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2div() { return &GenericExec<TPgDirectFunc<&int2div>, true, true, TArgs_int2div_Policy>; }
struct TArgs_int2eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2eq() { return &GenericExec<TPgDirectFunc<&int2eq>, true, true, TArgs_int2eq_Policy>; }
struct TArgs_int2ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2ge() { return &GenericExec<TPgDirectFunc<&int2ge>, true, true, TArgs_int2ge_Policy>; }
struct TArgs_int2gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2gt() { return &GenericExec<TPgDirectFunc<&int2gt>, true, true, TArgs_int2gt_Policy>; }
struct TArgs_int2in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2in() { return &GenericExec<TPgDirectFunc<&int2in>, true, true, TArgs_int2in_Policy>; }
struct TArgs_int2larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2larger() { return &GenericExec<TPgDirectFunc<&int2larger>, true, true, TArgs_int2larger_Policy>; }
struct TArgs_int2le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2le() { return &GenericExec<TPgDirectFunc<&int2le>, true, true, TArgs_int2le_Policy>; }
struct TArgs_int2lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2lt() { return &GenericExec<TPgDirectFunc<&int2lt>, true, true, TArgs_int2lt_Policy>; }
struct TArgs_int2mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mi() { return &GenericExec<TPgDirectFunc<&int2mi>, true, true, TArgs_int2mi_Policy>; }
struct TArgs_int2mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mod() { return &GenericExec<TPgDirectFunc<&int2mod>, true, true, TArgs_int2mod_Policy>; }
struct TArgs_int2mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mul() { return &GenericExec<TPgDirectFunc<&int2mul>, true, true, TArgs_int2mul_Policy>; }
struct TArgs_int2ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2ne() { return &GenericExec<TPgDirectFunc<&int2ne>, true, true, TArgs_int2ne_Policy>; }
struct TArgs_int2not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2not() { return &GenericExec<TPgDirectFunc<&int2not>, true, true, TArgs_int2not_Policy>; }
struct TArgs_int2or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2or() { return &GenericExec<TPgDirectFunc<&int2or>, true, true, TArgs_int2or_Policy>; }
struct TArgs_int2out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2out() { return &GenericExec<TPgDirectFunc<&int2out>, true, false, TArgs_int2out_Policy>; }
struct TArgs_int2pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2pl() { return &GenericExec<TPgDirectFunc<&int2pl>, true, true, TArgs_int2pl_Policy>; }
struct TArgs_int2recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2recv() { return &GenericExec<TPgDirectFunc<&int2recv>, true, true, TArgs_int2recv_Policy>; }
struct TArgs_int2send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2send() { return &GenericExec<TPgDirectFunc<&int2send>, true, false, TArgs_int2send_Policy>; }
struct TArgs_int2shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2shl() { return &GenericExec<TPgDirectFunc<&int2shl>, true, true, TArgs_int2shl_Policy>; }
struct TArgs_int2shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2shr() { return &GenericExec<TPgDirectFunc<&int2shr>, true, true, TArgs_int2shr_Policy>; }
struct TArgs_int2smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2smaller() { return &GenericExec<TPgDirectFunc<&int2smaller>, true, true, TArgs_int2smaller_Policy>; }
struct TArgs_int2um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2um() { return &GenericExec<TPgDirectFunc<&int2um>, true, true, TArgs_int2um_Policy>; }
struct TArgs_int2up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2up() { return &GenericExec<TPgDirectFunc<&int2up>, true, true, TArgs_int2up_Policy>; }
struct TArgs_int2vectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorin() { return &GenericExec<TPgDirectFunc<&int2vectorin>, true, false, TArgs_int2vectorin_Policy>; }
struct TArgs_int2vectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorout() { return &GenericExec<TPgDirectFunc<&int2vectorout>, true, false, TArgs_int2vectorout_Policy>; }
struct TArgs_int2vectorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2vectorrecv() { return &GenericExec<TPgDirectFunc<&int2vectorrecv>, true, false, TArgs_int2vectorrecv_Policy>; }
struct TArgs_int2vectorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorsend() { return &GenericExec<TPgDirectFunc<&int2vectorsend>, true, false, TArgs_int2vectorsend_Policy>; }
struct TArgs_int2xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2xor() { return &GenericExec<TPgDirectFunc<&int2xor>, true, true, TArgs_int2xor_Policy>; }
struct TArgs_int42div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42div() { return &GenericExec<TPgDirectFunc<&int42div>, true, true, TArgs_int42div_Policy>; }
struct TArgs_int42eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42eq() { return &GenericExec<TPgDirectFunc<&int42eq>, true, true, TArgs_int42eq_Policy>; }
struct TArgs_int42ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42ge() { return &GenericExec<TPgDirectFunc<&int42ge>, true, true, TArgs_int42ge_Policy>; }
struct TArgs_int42gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42gt() { return &GenericExec<TPgDirectFunc<&int42gt>, true, true, TArgs_int42gt_Policy>; }
struct TArgs_int42le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42le() { return &GenericExec<TPgDirectFunc<&int42le>, true, true, TArgs_int42le_Policy>; }
struct TArgs_int42lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42lt() { return &GenericExec<TPgDirectFunc<&int42lt>, true, true, TArgs_int42lt_Policy>; }
struct TArgs_int42mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42mi() { return &GenericExec<TPgDirectFunc<&int42mi>, true, true, TArgs_int42mi_Policy>; }
struct TArgs_int42mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42mul() { return &GenericExec<TPgDirectFunc<&int42mul>, true, true, TArgs_int42mul_Policy>; }
struct TArgs_int42ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42ne() { return &GenericExec<TPgDirectFunc<&int42ne>, true, true, TArgs_int42ne_Policy>; }
struct TArgs_int42pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42pl() { return &GenericExec<TPgDirectFunc<&int42pl>, true, true, TArgs_int42pl_Policy>; }
struct TArgs_int4_bool_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4_bool() { return &GenericExec<TPgDirectFunc<&int4_bool>, true, true, TArgs_int4_bool_Policy>; }
struct TArgs_int4abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4abs() { return &GenericExec<TPgDirectFunc<&int4abs>, true, true, TArgs_int4abs_Policy>; }
struct TArgs_int4and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4and() { return &GenericExec<TPgDirectFunc<&int4and>, true, true, TArgs_int4and_Policy>; }
struct TArgs_int4div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4div() { return &GenericExec<TPgDirectFunc<&int4div>, true, true, TArgs_int4div_Policy>; }
struct TArgs_int4eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4eq() { return &GenericExec<TPgDirectFunc<&int4eq>, true, true, TArgs_int4eq_Policy>; }
struct TArgs_int4gcd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4gcd() { return &GenericExec<TPgDirectFunc<&int4gcd>, true, true, TArgs_int4gcd_Policy>; }
struct TArgs_int4ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4ge() { return &GenericExec<TPgDirectFunc<&int4ge>, true, true, TArgs_int4ge_Policy>; }
struct TArgs_int4gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4gt() { return &GenericExec<TPgDirectFunc<&int4gt>, true, true, TArgs_int4gt_Policy>; }
struct TArgs_int4in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int4in() { return &GenericExec<TPgDirectFunc<&int4in>, true, true, TArgs_int4in_Policy>; }
struct TArgs_int4inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4inc() { return &GenericExec<TPgDirectFunc<&int4inc>, true, true, TArgs_int4inc_Policy>; }
struct TArgs_int4larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4larger() { return &GenericExec<TPgDirectFunc<&int4larger>, true, true, TArgs_int4larger_Policy>; }
struct TArgs_int4lcm_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4lcm() { return &GenericExec<TPgDirectFunc<&int4lcm>, true, true, TArgs_int4lcm_Policy>; }
struct TArgs_int4le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4le() { return &GenericExec<TPgDirectFunc<&int4le>, true, true, TArgs_int4le_Policy>; }
struct TArgs_int4lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4lt() { return &GenericExec<TPgDirectFunc<&int4lt>, true, true, TArgs_int4lt_Policy>; }
struct TArgs_int4mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mi() { return &GenericExec<TPgDirectFunc<&int4mi>, true, true, TArgs_int4mi_Policy>; }
struct TArgs_int4mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mod() { return &GenericExec<TPgDirectFunc<&int4mod>, true, true, TArgs_int4mod_Policy>; }
struct TArgs_int4mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mul() { return &GenericExec<TPgDirectFunc<&int4mul>, true, true, TArgs_int4mul_Policy>; }
struct TArgs_int4ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4ne() { return &GenericExec<TPgDirectFunc<&int4ne>, true, true, TArgs_int4ne_Policy>; }
struct TArgs_int4not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4not() { return &GenericExec<TPgDirectFunc<&int4not>, true, true, TArgs_int4not_Policy>; }
struct TArgs_int4or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4or() { return &GenericExec<TPgDirectFunc<&int4or>, true, true, TArgs_int4or_Policy>; }
struct TArgs_int4out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4out() { return &GenericExec<TPgDirectFunc<&int4out>, true, false, TArgs_int4out_Policy>; }
struct TArgs_int4pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4pl() { return &GenericExec<TPgDirectFunc<&int4pl>, true, true, TArgs_int4pl_Policy>; }
struct TArgs_int4range_canonical_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int4range_canonical() { return &GenericExec<TPgDirectFunc<&int4range_canonical>, true, false, TArgs_int4range_canonical_Policy>; }
struct TArgs_int4range_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4range_subdiff() { return &GenericExec<TPgDirectFunc<&int4range_subdiff>, true, true, TArgs_int4range_subdiff_Policy>; }
struct TArgs_int4recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4recv() { return &GenericExec<TPgDirectFunc<&int4recv>, true, true, TArgs_int4recv_Policy>; }
struct TArgs_int4send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4send() { return &GenericExec<TPgDirectFunc<&int4send>, true, false, TArgs_int4send_Policy>; }
struct TArgs_int4shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4shl() { return &GenericExec<TPgDirectFunc<&int4shl>, true, true, TArgs_int4shl_Policy>; }
struct TArgs_int4shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4shr() { return &GenericExec<TPgDirectFunc<&int4shr>, true, true, TArgs_int4shr_Policy>; }
struct TArgs_int4smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4smaller() { return &GenericExec<TPgDirectFunc<&int4smaller>, true, true, TArgs_int4smaller_Policy>; }
struct TArgs_int4um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4um() { return &GenericExec<TPgDirectFunc<&int4um>, true, true, TArgs_int4um_Policy>; }
struct TArgs_int4up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4up() { return &GenericExec<TPgDirectFunc<&int4up>, true, true, TArgs_int4up_Policy>; }
struct TArgs_int4xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4xor() { return &GenericExec<TPgDirectFunc<&int4xor>, true, true, TArgs_int4xor_Policy>; }
struct TArgs_int8range_canonical_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int8range_canonical() { return &GenericExec<TPgDirectFunc<&int8range_canonical>, true, false, TArgs_int8range_canonical_Policy>; }
struct TArgs_int8range_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8range_subdiff() { return &GenericExec<TPgDirectFunc<&int8range_subdiff>, true, true, TArgs_int8range_subdiff_Policy>; }
struct TArgs_interval_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_time() { return &GenericExec<TPgDirectFunc<&interval_time>, true, true, TArgs_interval_time_Policy>; }
struct TArgs_jsonb_path_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_exists() { return &GenericExec<TPgDirectFunc<&jsonb_path_exists>, true, true, TArgs_jsonb_path_exists_Policy>; }
struct TArgs_jsonb_path_exists_opr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_path_exists_opr() { return &GenericExec<TPgDirectFunc<&jsonb_path_exists_opr>, true, true, TArgs_jsonb_path_exists_opr_Policy>; }
struct TArgs_jsonb_path_exists_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_exists_tz() { return &GenericExec<TPgDirectFunc<&jsonb_path_exists_tz>, true, true, TArgs_jsonb_path_exists_tz_Policy>; }
struct TArgs_jsonb_path_match_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_match() { return &GenericExec<TPgDirectFunc<&jsonb_path_match>, true, true, TArgs_jsonb_path_match_Policy>; }
struct TArgs_jsonb_path_match_opr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_path_match_opr() { return &GenericExec<TPgDirectFunc<&jsonb_path_match_opr>, true, true, TArgs_jsonb_path_match_opr_Policy>; }
struct TArgs_jsonb_path_match_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_match_tz() { return &GenericExec<TPgDirectFunc<&jsonb_path_match_tz>, true, true, TArgs_jsonb_path_match_tz_Policy>; }
struct TArgs_jsonb_path_query_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_array() { return &GenericExec<TPgDirectFunc<&jsonb_path_query_array>, true, false, TArgs_jsonb_path_query_array_Policy>; }
struct TArgs_jsonb_path_query_array_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_array_tz() { return &GenericExec<TPgDirectFunc<&jsonb_path_query_array_tz>, true, false, TArgs_jsonb_path_query_array_tz_Policy>; }
struct TArgs_jsonb_path_query_first_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_first() { return &GenericExec<TPgDirectFunc<&jsonb_path_query_first>, true, false, TArgs_jsonb_path_query_first_Policy>; }
struct TArgs_jsonb_path_query_first_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_first_tz() { return &GenericExec<TPgDirectFunc<&jsonb_path_query_first_tz>, true, false, TArgs_jsonb_path_query_first_tz_Policy>; }
struct TArgs_jsonb_subscript_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonb_subscript_handler() { return &GenericExec<TPgDirectFunc<&jsonb_subscript_handler>, true, true, TArgs_jsonb_subscript_handler_Policy>; }
struct TArgs_make_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_make_date() { return &GenericExec<TPgDirectFunc<&make_date>, true, true, TArgs_make_date_Policy>; }
struct TArgs_make_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_make_time() { return &GenericExec<TPgDirectFunc<&make_time>, true, true, TArgs_make_time_Policy>; }
struct TArgs_mxid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_mxid_age() { return &GenericExec<TPgDirectFunc<&mxid_age>, true, true, TArgs_mxid_age_Policy>; }
struct TArgs_name_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_name_bpchar() { return &GenericExec<TPgDirectFunc<&name_bpchar>, true, false, TArgs_name_bpchar_Policy>; }
struct TArgs_networkjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_networkjoinsel() { return &GenericExec<TPgDirectFunc<&networkjoinsel>, true, true, TArgs_networkjoinsel_Policy>; }
struct TArgs_networksel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_networksel() { return &GenericExec<TPgDirectFunc<&networksel>, true, true, TArgs_networksel_Policy>; }
struct TArgs_numrange_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numrange_subdiff() { return &GenericExec<TPgDirectFunc<&numrange_subdiff>, true, true, TArgs_numrange_subdiff_Policy>; }
struct TArgs_overlaps_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_overlaps_time() { return &GenericExec<TPgDirectFunc<&overlaps_time>, false, true, TArgs_overlaps_time_Policy>; }
struct TArgs_overlaps_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
TExecFunc arrow_overlaps_timetz() { return &GenericExec<TPgDirectFunc<&overlaps_timetz>, false, true, TArgs_overlaps_timetz_Policy>; }
struct TArgs_parse_ident_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_parse_ident() { return &GenericExec<TPgDirectFunc<&parse_ident>, true, false, TArgs_parse_ident_Policy>; }
struct TArgs_pg_collation_for_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_for() { return &GenericExec<TPgDirectFunc<&pg_collation_for>, false, false, TArgs_pg_collation_for_Policy>; }
struct TArgs_pg_collation_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_is_visible() { return &GenericExec<TPgDirectFunc<&pg_collation_is_visible>, true, true, TArgs_pg_collation_is_visible_Policy>; }
struct TArgs_pg_column_is_updatable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_column_is_updatable() { return &GenericExec<TPgDirectFunc<&pg_column_is_updatable>, true, true, TArgs_pg_column_is_updatable_Policy>; }
struct TArgs_pg_conversion_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_conversion_is_visible() { return &GenericExec<TPgDirectFunc<&pg_conversion_is_visible>, true, true, TArgs_pg_conversion_is_visible_Policy>; }
struct TArgs_pg_current_logfile_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_logfile() { return &GenericExec<TPgDirectFunc<&pg_current_logfile>, false, false, TArgs_pg_current_logfile_Policy>; }
struct TArgs_pg_current_logfile_1arg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_current_logfile_1arg() { return &GenericExec<TPgDirectFunc<&pg_current_logfile_1arg>, false, false, TArgs_pg_current_logfile_1arg_Policy>; }
struct TArgs_pg_dependencies_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_in() { return &GenericExec<TPgDirectFunc<&pg_dependencies_in>, true, false, TArgs_pg_dependencies_in_Policy>; }
struct TArgs_pg_dependencies_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_out() { return &GenericExec<TPgDirectFunc<&pg_dependencies_out>, true, false, TArgs_pg_dependencies_out_Policy>; }
struct TArgs_pg_dependencies_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_dependencies_recv() { return &GenericExec<TPgDirectFunc<&pg_dependencies_recv>, true, false, TArgs_pg_dependencies_recv_Policy>; }
struct TArgs_pg_dependencies_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_send() { return &GenericExec<TPgDirectFunc<&pg_dependencies_send>, true, false, TArgs_pg_dependencies_send_Policy>; }
struct TArgs_pg_event_trigger_table_rewrite_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_event_trigger_table_rewrite_oid() { return &GenericExec<TPgDirectFunc<&pg_event_trigger_table_rewrite_oid>, true, true, TArgs_pg_event_trigger_table_rewrite_oid_Policy>; }
struct TArgs_pg_event_trigger_table_rewrite_reason_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_event_trigger_table_rewrite_reason() { return &GenericExec<TPgDirectFunc<&pg_event_trigger_table_rewrite_reason>, true, true, TArgs_pg_event_trigger_table_rewrite_reason_Policy>; }
struct TArgs_pg_export_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_export_snapshot() { return &GenericExec<TPgDirectFunc<&pg_export_snapshot>, true, false, TArgs_pg_export_snapshot_Policy>; }
struct TArgs_pg_function_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_function_is_visible() { return &GenericExec<TPgDirectFunc<&pg_function_is_visible>, true, true, TArgs_pg_function_is_visible_Policy>; }
struct TArgs_pg_get_constraintdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_constraintdef() { return &GenericExec<TPgDirectFunc<&pg_get_constraintdef>, true, false, TArgs_pg_get_constraintdef_Policy>; }
struct TArgs_pg_get_constraintdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_constraintdef_ext() { return &GenericExec<TPgDirectFunc<&pg_get_constraintdef_ext>, true, false, TArgs_pg_get_constraintdef_ext_Policy>; }
struct TArgs_pg_get_expr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_get_expr() { return &GenericExec<TPgDirectFunc<&pg_get_expr>, true, false, TArgs_pg_get_expr_Policy>; }
struct TArgs_pg_get_expr_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_get_expr_ext() { return &GenericExec<TPgDirectFunc<&pg_get_expr_ext>, true, false, TArgs_pg_get_expr_ext_Policy>; }
struct TArgs_pg_get_function_arg_default_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_function_arg_default() { return &GenericExec<TPgDirectFunc<&pg_get_function_arg_default>, true, false, TArgs_pg_get_function_arg_default_Policy>; }
struct TArgs_pg_get_function_arguments_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_arguments() { return &GenericExec<TPgDirectFunc<&pg_get_function_arguments>, true, false, TArgs_pg_get_function_arguments_Policy>; }
struct TArgs_pg_get_function_identity_arguments_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_identity_arguments() { return &GenericExec<TPgDirectFunc<&pg_get_function_identity_arguments>, true, false, TArgs_pg_get_function_identity_arguments_Policy>; }
struct TArgs_pg_get_function_result_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_result() { return &GenericExec<TPgDirectFunc<&pg_get_function_result>, true, false, TArgs_pg_get_function_result_Policy>; }
struct TArgs_pg_get_function_sqlbody_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_sqlbody() { return &GenericExec<TPgDirectFunc<&pg_get_function_sqlbody>, true, false, TArgs_pg_get_function_sqlbody_Policy>; }
struct TArgs_pg_get_functiondef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_functiondef() { return &GenericExec<TPgDirectFunc<&pg_get_functiondef>, true, false, TArgs_pg_get_functiondef_Policy>; }
struct TArgs_pg_get_indexdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_indexdef() { return &GenericExec<TPgDirectFunc<&pg_get_indexdef>, true, false, TArgs_pg_get_indexdef_Policy>; }
struct TArgs_pg_get_indexdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_get_indexdef_ext() { return &GenericExec<TPgDirectFunc<&pg_get_indexdef_ext>, true, false, TArgs_pg_get_indexdef_ext_Policy>; }
struct TArgs_pg_get_partition_constraintdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_partition_constraintdef() { return &GenericExec<TPgDirectFunc<&pg_get_partition_constraintdef>, true, false, TArgs_pg_get_partition_constraintdef_Policy>; }
struct TArgs_pg_get_partkeydef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_partkeydef() { return &GenericExec<TPgDirectFunc<&pg_get_partkeydef>, true, false, TArgs_pg_get_partkeydef_Policy>; }
struct TArgs_pg_get_replica_identity_index_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_replica_identity_index() { return &GenericExec<TPgDirectFunc<&pg_get_replica_identity_index>, true, true, TArgs_pg_get_replica_identity_index_Policy>; }
struct TArgs_pg_get_ruledef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_ruledef() { return &GenericExec<TPgDirectFunc<&pg_get_ruledef>, true, false, TArgs_pg_get_ruledef_Policy>; }
struct TArgs_pg_get_ruledef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_ruledef_ext() { return &GenericExec<TPgDirectFunc<&pg_get_ruledef_ext>, true, false, TArgs_pg_get_ruledef_ext_Policy>; }
struct TArgs_pg_get_serial_sequence_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_get_serial_sequence() { return &GenericExec<TPgDirectFunc<&pg_get_serial_sequence>, true, false, TArgs_pg_get_serial_sequence_Policy>; }
struct TArgs_pg_get_statisticsobjdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef() { return &GenericExec<TPgDirectFunc<&pg_get_statisticsobjdef>, true, false, TArgs_pg_get_statisticsobjdef_Policy>; }
struct TArgs_pg_get_statisticsobjdef_columns_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef_columns() { return &GenericExec<TPgDirectFunc<&pg_get_statisticsobjdef_columns>, true, false, TArgs_pg_get_statisticsobjdef_columns_Policy>; }
struct TArgs_pg_get_statisticsobjdef_expressions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef_expressions() { return &GenericExec<TPgDirectFunc<&pg_get_statisticsobjdef_expressions>, true, false, TArgs_pg_get_statisticsobjdef_expressions_Policy>; }
struct TArgs_pg_get_triggerdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_triggerdef() { return &GenericExec<TPgDirectFunc<&pg_get_triggerdef>, true, false, TArgs_pg_get_triggerdef_Policy>; }
struct TArgs_pg_get_triggerdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_triggerdef_ext() { return &GenericExec<TPgDirectFunc<&pg_get_triggerdef_ext>, true, false, TArgs_pg_get_triggerdef_ext_Policy>; }
struct TArgs_pg_get_userbyid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_userbyid() { return &GenericExec<TPgDirectFunc<&pg_get_userbyid>, true, false, TArgs_pg_get_userbyid_Policy>; }
struct TArgs_pg_get_viewdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_viewdef() { return &GenericExec<TPgDirectFunc<&pg_get_viewdef>, true, false, TArgs_pg_get_viewdef_Policy>; }
struct TArgs_pg_get_viewdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_viewdef_ext() { return &GenericExec<TPgDirectFunc<&pg_get_viewdef_ext>, true, false, TArgs_pg_get_viewdef_ext_Policy>; }
struct TArgs_pg_get_viewdef_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_get_viewdef_name() { return &GenericExec<TPgDirectFunc<&pg_get_viewdef_name>, true, false, TArgs_pg_get_viewdef_name_Policy>; }
struct TArgs_pg_get_viewdef_name_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_get_viewdef_name_ext() { return &GenericExec<TPgDirectFunc<&pg_get_viewdef_name_ext>, true, false, TArgs_pg_get_viewdef_name_ext_Policy>; }
struct TArgs_pg_get_viewdef_wrap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_viewdef_wrap() { return &GenericExec<TPgDirectFunc<&pg_get_viewdef_wrap>, true, false, TArgs_pg_get_viewdef_wrap_Policy>; }
struct TArgs_pg_is_other_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_is_other_temp_schema() { return &GenericExec<TPgDirectFunc<&pg_is_other_temp_schema>, true, true, TArgs_pg_is_other_temp_schema_Policy>; }
struct TArgs_pg_log_backend_memory_contexts_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_log_backend_memory_contexts() { return &GenericExec<TPgDirectFunc<&pg_log_backend_memory_contexts>, true, true, TArgs_pg_log_backend_memory_contexts_Policy>; }
struct TArgs_pg_mcv_list_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_in() { return &GenericExec<TPgDirectFunc<&pg_mcv_list_in>, true, false, TArgs_pg_mcv_list_in_Policy>; }
struct TArgs_pg_mcv_list_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_out() { return &GenericExec<TPgDirectFunc<&pg_mcv_list_out>, true, false, TArgs_pg_mcv_list_out_Policy>; }
struct TArgs_pg_mcv_list_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_mcv_list_recv() { return &GenericExec<TPgDirectFunc<&pg_mcv_list_recv>, true, false, TArgs_pg_mcv_list_recv_Policy>; }
struct TArgs_pg_mcv_list_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_send() { return &GenericExec<TPgDirectFunc<&pg_mcv_list_send>, true, false, TArgs_pg_mcv_list_send_Policy>; }
struct TArgs_pg_my_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_my_temp_schema() { return &GenericExec<TPgDirectFunc<&pg_my_temp_schema>, true, true, TArgs_pg_my_temp_schema_Policy>; }
struct TArgs_pg_ndistinct_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_ndistinct_in() { return &GenericExec<TPgDirectFunc<&pg_ndistinct_in>, true, false, TArgs_pg_ndistinct_in_Policy>; }
struct TArgs_pg_ndistinct_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_ndistinct_out() { return &GenericExec<TPgDirectFunc<&pg_ndistinct_out>, true, false, TArgs_pg_ndistinct_out_Policy>; }
struct TArgs_pg_ndistinct_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ndistinct_recv() { return &GenericExec<TPgDirectFunc<&pg_ndistinct_recv>, true, false, TArgs_pg_ndistinct_recv_Policy>; }
struct TArgs_pg_ndistinct_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_ndistinct_send() { return &GenericExec<TPgDirectFunc<&pg_ndistinct_send>, true, false, TArgs_pg_ndistinct_send_Policy>; }
struct TArgs_pg_opclass_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_opclass_is_visible() { return &GenericExec<TPgDirectFunc<&pg_opclass_is_visible>, true, true, TArgs_pg_opclass_is_visible_Policy>; }
struct TArgs_pg_operator_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_operator_is_visible() { return &GenericExec<TPgDirectFunc<&pg_operator_is_visible>, true, true, TArgs_pg_operator_is_visible_Policy>; }
struct TArgs_pg_opfamily_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_opfamily_is_visible() { return &GenericExec<TPgDirectFunc<&pg_opfamily_is_visible>, true, true, TArgs_pg_opfamily_is_visible_Policy>; }
struct TArgs_pg_relation_is_updatable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_relation_is_updatable() { return &GenericExec<TPgDirectFunc<&pg_relation_is_updatable>, true, true, TArgs_pg_relation_is_updatable_Policy>; }
struct TArgs_pg_sleep_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sleep() { return &GenericExec<TPgDirectFunc<&pg_sleep>, true, true, TArgs_pg_sleep_Policy>; }
struct TArgs_pg_statistics_obj_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_statistics_obj_is_visible() { return &GenericExec<TPgDirectFunc<&pg_statistics_obj_is_visible>, true, true, TArgs_pg_statistics_obj_is_visible_Policy>; }
struct TArgs_pg_table_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_is_visible() { return &GenericExec<TPgDirectFunc<&pg_table_is_visible>, true, true, TArgs_pg_table_is_visible_Policy>; }
struct TArgs_pg_tablespace_location_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_tablespace_location() { return &GenericExec<TPgDirectFunc<&pg_tablespace_location>, true, false, TArgs_pg_tablespace_location_Policy>; }
struct TArgs_pg_ts_config_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_config_is_visible() { return &GenericExec<TPgDirectFunc<&pg_ts_config_is_visible>, true, true, TArgs_pg_ts_config_is_visible_Policy>; }
struct TArgs_pg_ts_dict_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_dict_is_visible() { return &GenericExec<TPgDirectFunc<&pg_ts_dict_is_visible>, true, true, TArgs_pg_ts_dict_is_visible_Policy>; }
struct TArgs_pg_ts_parser_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_parser_is_visible() { return &GenericExec<TPgDirectFunc<&pg_ts_parser_is_visible>, true, true, TArgs_pg_ts_parser_is_visible_Policy>; }
struct TArgs_pg_ts_template_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_template_is_visible() { return &GenericExec<TPgDirectFunc<&pg_ts_template_is_visible>, true, true, TArgs_pg_ts_template_is_visible_Policy>; }
struct TArgs_pg_type_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_type_is_visible() { return &GenericExec<TPgDirectFunc<&pg_type_is_visible>, true, true, TArgs_pg_type_is_visible_Policy>; }
struct TArgs_pg_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_typeof() { return &GenericExec<TPgDirectFunc<&pg_typeof>, false, true, TArgs_pg_typeof_Policy>; }
struct TArgs_prsd_end_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_prsd_end() { return &GenericExec<TPgDirectFunc<&prsd_end>, true, true, TArgs_prsd_end_Policy>; }
struct TArgs_prsd_headline_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_prsd_headline() { return &GenericExec<TPgDirectFunc<&prsd_headline>, true, true, TArgs_prsd_headline_Policy>; }
struct TArgs_prsd_lextype_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_prsd_lextype() { return &GenericExec<TPgDirectFunc<&prsd_lextype>, true, true, TArgs_prsd_lextype_Policy>; }
struct TArgs_prsd_nexttoken_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_prsd_nexttoken() { return &GenericExec<TPgDirectFunc<&prsd_nexttoken>, true, true, TArgs_prsd_nexttoken_Policy>; }
struct TArgs_prsd_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_prsd_start() { return &GenericExec<TPgDirectFunc<&prsd_start>, true, true, TArgs_prsd_start_Policy>; }
struct TArgs_query_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xml() { return &GenericExec<TPgDirectFunc<&query_to_xml>, true, false, TArgs_query_to_xml_Policy>; }
struct TArgs_query_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xml_and_xmlschema() { return &GenericExec<TPgDirectFunc<&query_to_xml_and_xmlschema>, true, false, TArgs_query_to_xml_and_xmlschema_Policy>; }
struct TArgs_query_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xmlschema() { return &GenericExec<TPgDirectFunc<&query_to_xmlschema>, true, false, TArgs_query_to_xmlschema_Policy>; }
struct TArgs_range_adjacent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_adjacent() { return &GenericExec<TPgDirectFunc<&range_adjacent>, true, true, TArgs_range_adjacent_Policy>; }
struct TArgs_range_after_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_after() { return &GenericExec<TPgDirectFunc<&range_after>, true, true, TArgs_range_after_Policy>; }
struct TArgs_range_before_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_before() { return &GenericExec<TPgDirectFunc<&range_before>, true, true, TArgs_range_before_Policy>; }
struct TArgs_range_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_cmp() { return &GenericExec<TPgDirectFunc<&range_cmp>, true, true, TArgs_range_cmp_Policy>; }
TExecFunc arrow_range_constructor2() { return &GenericExec<TPgDirectFunc<&range_constructor2>, false, false, TDefaultArgsPolicy>; }
TExecFunc arrow_range_constructor3() { return &GenericExec<TPgDirectFunc<&range_constructor3>, false, false, TDefaultArgsPolicy>; }
struct TArgs_range_contained_by_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contained_by() { return &GenericExec<TPgDirectFunc<&range_contained_by>, true, true, TArgs_range_contained_by_Policy>; }
struct TArgs_range_contains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contains() { return &GenericExec<TPgDirectFunc<&range_contains>, true, true, TArgs_range_contains_Policy>; }
struct TArgs_range_contains_elem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_range_contains_elem() { return &GenericExec<TPgDirectFunc<&range_contains_elem>, true, true, TArgs_range_contains_elem_Policy>; }
struct TArgs_range_empty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_empty() { return &GenericExec<TPgDirectFunc<&range_empty>, true, true, TArgs_range_empty_Policy>; }
struct TArgs_range_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_eq() { return &GenericExec<TPgDirectFunc<&range_eq>, true, true, TArgs_range_eq_Policy>; }
struct TArgs_range_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_ge() { return &GenericExec<TPgDirectFunc<&range_ge>, true, true, TArgs_range_ge_Policy>; }
struct TArgs_range_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_gt() { return &GenericExec<TPgDirectFunc<&range_gt>, true, true, TArgs_range_gt_Policy>; }
struct TArgs_range_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_range_in() { return &GenericExec<TPgDirectFunc<&range_in>, true, false, TArgs_range_in_Policy>; }
struct TArgs_range_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_intersect() { return &GenericExec<TPgDirectFunc<&range_intersect>, true, false, TArgs_range_intersect_Policy>; }
struct TArgs_range_intersect_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_intersect_agg_transfn() { return &GenericExec<TPgDirectFunc<&range_intersect_agg_transfn>, true, false, TArgs_range_intersect_agg_transfn_Policy>; }
struct TArgs_range_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_le() { return &GenericExec<TPgDirectFunc<&range_le>, true, true, TArgs_range_le_Policy>; }
struct TArgs_range_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_lower() { return &GenericExec<TPgDirectFunc<&range_lower>, true, true, TArgs_range_lower_Policy>; }
struct TArgs_range_lower_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_lower_inc() { return &GenericExec<TPgDirectFunc<&range_lower_inc>, true, true, TArgs_range_lower_inc_Policy>; }
struct TArgs_range_lower_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_lower_inf() { return &GenericExec<TPgDirectFunc<&range_lower_inf>, true, true, TArgs_range_lower_inf_Policy>; }
struct TArgs_range_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_lt() { return &GenericExec<TPgDirectFunc<&range_lt>, true, true, TArgs_range_lt_Policy>; }
struct TArgs_range_merge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_merge() { return &GenericExec<TPgDirectFunc<&range_merge>, true, false, TArgs_range_merge_Policy>; }
struct TArgs_range_minus_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_minus() { return &GenericExec<TPgDirectFunc<&range_minus>, true, false, TArgs_range_minus_Policy>; }
struct TArgs_range_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_ne() { return &GenericExec<TPgDirectFunc<&range_ne>, true, true, TArgs_range_ne_Policy>; }
struct TArgs_range_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_out() { return &GenericExec<TPgDirectFunc<&range_out>, true, false, TArgs_range_out_Policy>; }
struct TArgs_range_overlaps_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overlaps() { return &GenericExec<TPgDirectFunc<&range_overlaps>, true, true, TArgs_range_overlaps_Policy>; }
struct TArgs_range_overleft_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overleft() { return &GenericExec<TPgDirectFunc<&range_overleft>, true, true, TArgs_range_overleft_Policy>; }
struct TArgs_range_overright_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overright() { return &GenericExec<TPgDirectFunc<&range_overright>, true, true, TArgs_range_overright_Policy>; }
struct TArgs_range_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_range_recv() { return &GenericExec<TPgDirectFunc<&range_recv>, true, false, TArgs_range_recv_Policy>; }
struct TArgs_range_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_send() { return &GenericExec<TPgDirectFunc<&range_send>, true, false, TArgs_range_send_Policy>; }
struct TArgs_range_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_union() { return &GenericExec<TPgDirectFunc<&range_union>, true, false, TArgs_range_union_Policy>; }
struct TArgs_range_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_upper() { return &GenericExec<TPgDirectFunc<&range_upper>, true, true, TArgs_range_upper_Policy>; }
struct TArgs_range_upper_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_upper_inc() { return &GenericExec<TPgDirectFunc<&range_upper_inc>, true, true, TArgs_range_upper_inc_Policy>; }
struct TArgs_range_upper_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_upper_inf() { return &GenericExec<TPgDirectFunc<&range_upper_inf>, true, true, TArgs_range_upper_inf_Policy>; }
struct TArgs_row_security_active_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_row_security_active() { return &GenericExec<TPgDirectFunc<&row_security_active>, true, true, TArgs_row_security_active_Policy>; }
struct TArgs_row_security_active_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_row_security_active_name() { return &GenericExec<TPgDirectFunc<&row_security_active_name>, true, true, TArgs_row_security_active_name_Policy>; }
struct TArgs_schema_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xml() { return &GenericExec<TPgDirectFunc<&schema_to_xml>, true, false, TArgs_schema_to_xml_Policy>; }
struct TArgs_schema_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xml_and_xmlschema() { return &GenericExec<TPgDirectFunc<&schema_to_xml_and_xmlschema>, true, false, TArgs_schema_to_xml_and_xmlschema_Policy>; }
struct TArgs_schema_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xmlschema() { return &GenericExec<TPgDirectFunc<&schema_to_xmlschema>, true, false, TArgs_schema_to_xmlschema_Policy>; }
struct TArgs_spg_bbox_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_bbox_quad_config() { return &GenericExec<TPgDirectFunc<&spg_bbox_quad_config>, true, true, TArgs_spg_bbox_quad_config_Policy>; }
struct TArgs_spg_box_quad_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_box_quad_choose() { return &GenericExec<TPgDirectFunc<&spg_box_quad_choose>, true, true, TArgs_spg_box_quad_choose_Policy>; }
struct TArgs_spg_box_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_box_quad_config() { return &GenericExec<TPgDirectFunc<&spg_box_quad_config>, true, true, TArgs_spg_box_quad_config_Policy>; }
struct TArgs_spg_box_quad_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_box_quad_inner_consistent() { return &GenericExec<TPgDirectFunc<&spg_box_quad_inner_consistent>, true, true, TArgs_spg_box_quad_inner_consistent_Policy>; }
struct TArgs_spg_box_quad_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_box_quad_leaf_consistent() { return &GenericExec<TPgDirectFunc<&spg_box_quad_leaf_consistent>, true, true, TArgs_spg_box_quad_leaf_consistent_Policy>; }
struct TArgs_spg_box_quad_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_box_quad_picksplit() { return &GenericExec<TPgDirectFunc<&spg_box_quad_picksplit>, true, true, TArgs_spg_box_quad_picksplit_Policy>; }
struct TArgs_spg_poly_quad_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_spg_poly_quad_compress() { return &GenericExec<TPgDirectFunc<&spg_poly_quad_compress>, true, false, TArgs_spg_poly_quad_compress_Policy>; }
struct TArgs_spg_quad_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_choose() { return &GenericExec<TPgDirectFunc<&spg_quad_choose>, true, true, TArgs_spg_quad_choose_Policy>; }
struct TArgs_spg_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_config() { return &GenericExec<TPgDirectFunc<&spg_quad_config>, true, true, TArgs_spg_quad_config_Policy>; }
struct TArgs_spg_quad_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_inner_consistent() { return &GenericExec<TPgDirectFunc<&spg_quad_inner_consistent>, true, true, TArgs_spg_quad_inner_consistent_Policy>; }
struct TArgs_spg_quad_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_leaf_consistent() { return &GenericExec<TPgDirectFunc<&spg_quad_leaf_consistent>, true, true, TArgs_spg_quad_leaf_consistent_Policy>; }
struct TArgs_spg_quad_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_picksplit() { return &GenericExec<TPgDirectFunc<&spg_quad_picksplit>, true, true, TArgs_spg_quad_picksplit_Policy>; }
struct TArgs_spg_range_quad_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_choose() { return &GenericExec<TPgDirectFunc<&spg_range_quad_choose>, true, true, TArgs_spg_range_quad_choose_Policy>; }
struct TArgs_spg_range_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_config() { return &GenericExec<TPgDirectFunc<&spg_range_quad_config>, true, true, TArgs_spg_range_quad_config_Policy>; }
struct TArgs_spg_range_quad_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_inner_consistent() { return &GenericExec<TPgDirectFunc<&spg_range_quad_inner_consistent>, true, true, TArgs_spg_range_quad_inner_consistent_Policy>; }
struct TArgs_spg_range_quad_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_leaf_consistent() { return &GenericExec<TPgDirectFunc<&spg_range_quad_leaf_consistent>, true, true, TArgs_spg_range_quad_leaf_consistent_Policy>; }
struct TArgs_spg_range_quad_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_picksplit() { return &GenericExec<TPgDirectFunc<&spg_range_quad_picksplit>, true, true, TArgs_spg_range_quad_picksplit_Policy>; }
struct TArgs_spg_text_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_choose() { return &GenericExec<TPgDirectFunc<&spg_text_choose>, true, true, TArgs_spg_text_choose_Policy>; }
struct TArgs_spg_text_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_config() { return &GenericExec<TPgDirectFunc<&spg_text_config>, true, true, TArgs_spg_text_config_Policy>; }
struct TArgs_spg_text_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_inner_consistent() { return &GenericExec<TPgDirectFunc<&spg_text_inner_consistent>, true, true, TArgs_spg_text_inner_consistent_Policy>; }
struct TArgs_spg_text_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_leaf_consistent() { return &GenericExec<TPgDirectFunc<&spg_text_leaf_consistent>, true, true, TArgs_spg_text_leaf_consistent_Policy>; }
struct TArgs_spg_text_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_picksplit() { return &GenericExec<TPgDirectFunc<&spg_text_picksplit>, true, true, TArgs_spg_text_picksplit_Policy>; }
struct TArgs_table_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xml() { return &GenericExec<TPgDirectFunc<&table_to_xml>, true, false, TArgs_table_to_xml_Policy>; }
struct TArgs_table_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xml_and_xmlschema() { return &GenericExec<TPgDirectFunc<&table_to_xml_and_xmlschema>, true, false, TArgs_table_to_xml_and_xmlschema_Policy>; }
struct TArgs_table_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xmlschema() { return &GenericExec<TPgDirectFunc<&table_to_xmlschema>, true, false, TArgs_table_to_xmlschema_Policy>; }
struct TArgs_text_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_char() { return &GenericExec<TPgDirectFunc<&text_char>, true, true, TArgs_text_char_Policy>; }
struct TArgs_texttoxml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_texttoxml() { return &GenericExec<TPgDirectFunc<&texttoxml>, true, false, TArgs_texttoxml_Policy>; }
struct TArgs_thesaurus_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_thesaurus_init() { return &GenericExec<TPgDirectFunc<&thesaurus_init>, true, true, TArgs_thesaurus_init_Policy>; }
struct TArgs_thesaurus_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_thesaurus_lexize() { return &GenericExec<TPgDirectFunc<&thesaurus_lexize>, true, true, TArgs_thesaurus_lexize_Policy>; }
struct TArgs_tideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tideq() { return &GenericExec<TPgDirectFunc<&tideq>, true, true, TArgs_tideq_Policy>; }
struct TArgs_tidge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidge() { return &GenericExec<TPgDirectFunc<&tidge>, true, true, TArgs_tidge_Policy>; }
struct TArgs_tidgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidgt() { return &GenericExec<TPgDirectFunc<&tidgt>, true, true, TArgs_tidgt_Policy>; }
struct TArgs_tidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tidin() { return &GenericExec<TPgDirectFunc<&tidin>, true, false, TArgs_tidin_Policy>; }
struct TArgs_tidlarger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidlarger() { return &GenericExec<TPgDirectFunc<&tidlarger>, true, false, TArgs_tidlarger_Policy>; }
struct TArgs_tidle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidle() { return &GenericExec<TPgDirectFunc<&tidle>, true, true, TArgs_tidle_Policy>; }
struct TArgs_tidlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidlt() { return &GenericExec<TPgDirectFunc<&tidlt>, true, true, TArgs_tidlt_Policy>; }
struct TArgs_tidne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidne() { return &GenericExec<TPgDirectFunc<&tidne>, true, true, TArgs_tidne_Policy>; }
struct TArgs_tidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tidout() { return &GenericExec<TPgDirectFunc<&tidout>, true, false, TArgs_tidout_Policy>; }
struct TArgs_tidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tidrecv() { return &GenericExec<TPgDirectFunc<&tidrecv>, true, false, TArgs_tidrecv_Policy>; }
struct TArgs_tidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tidsend() { return &GenericExec<TPgDirectFunc<&tidsend>, true, false, TArgs_tidsend_Policy>; }
struct TArgs_tidsmaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tidsmaller() { return &GenericExec<TPgDirectFunc<&tidsmaller>, true, false, TArgs_tidsmaller_Policy>; }
struct TArgs_time_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_cmp() { return &GenericExec<TPgDirectFunc<&time_cmp>, true, true, TArgs_time_cmp_Policy>; }
struct TArgs_time_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_eq() { return &GenericExec<TPgDirectFunc<&time_eq>, true, true, TArgs_time_eq_Policy>; }
struct TArgs_time_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_ge() { return &GenericExec<TPgDirectFunc<&time_ge>, true, true, TArgs_time_ge_Policy>; }
struct TArgs_time_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_gt() { return &GenericExec<TPgDirectFunc<&time_gt>, true, true, TArgs_time_gt_Policy>; }
struct TArgs_time_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_hash() { return &GenericExec<TPgDirectFunc<&time_hash>, true, true, TArgs_time_hash_Policy>; }
struct TArgs_time_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_hash_extended() { return &GenericExec<TPgDirectFunc<&time_hash_extended>, true, true, TArgs_time_hash_extended_Policy>; }
struct TArgs_time_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_time_in() { return &GenericExec<TPgDirectFunc<&time_in>, true, true, TArgs_time_in_Policy>; }
struct TArgs_time_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_interval() { return &GenericExec<TPgDirectFunc<&time_interval>, true, false, TArgs_time_interval_Policy>; }
struct TArgs_time_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_larger() { return &GenericExec<TPgDirectFunc<&time_larger>, true, true, TArgs_time_larger_Policy>; }
struct TArgs_time_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_le() { return &GenericExec<TPgDirectFunc<&time_le>, true, true, TArgs_time_le_Policy>; }
struct TArgs_time_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_lt() { return &GenericExec<TPgDirectFunc<&time_lt>, true, true, TArgs_time_lt_Policy>; }
struct TArgs_time_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_time_mi_interval() { return &GenericExec<TPgDirectFunc<&time_mi_interval>, true, true, TArgs_time_mi_interval_Policy>; }
struct TArgs_time_mi_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_mi_time() { return &GenericExec<TPgDirectFunc<&time_mi_time>, true, false, TArgs_time_mi_time_Policy>; }
struct TArgs_time_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_ne() { return &GenericExec<TPgDirectFunc<&time_ne>, true, true, TArgs_time_ne_Policy>; }
struct TArgs_time_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_out() { return &GenericExec<TPgDirectFunc<&time_out>, true, false, TArgs_time_out_Policy>; }
struct TArgs_time_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_time_part() { return &GenericExec<TPgDirectFunc<&time_part>, true, true, TArgs_time_part_Policy>; }
struct TArgs_time_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_time_pl_interval() { return &GenericExec<TPgDirectFunc<&time_pl_interval>, true, true, TArgs_time_pl_interval_Policy>; }
struct TArgs_time_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_time_recv() { return &GenericExec<TPgDirectFunc<&time_recv>, true, true, TArgs_time_recv_Policy>; }
struct TArgs_time_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_scale() { return &GenericExec<TPgDirectFunc<&time_scale>, true, true, TArgs_time_scale_Policy>; }
struct TArgs_time_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_send() { return &GenericExec<TPgDirectFunc<&time_send>, true, false, TArgs_time_send_Policy>; }
struct TArgs_time_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_smaller() { return &GenericExec<TPgDirectFunc<&time_smaller>, true, true, TArgs_time_smaller_Policy>; }
struct TArgs_time_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_support() { return &GenericExec<TPgDirectFunc<&time_support>, true, true, TArgs_time_support_Policy>; }
struct TArgs_time_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_timetz() { return &GenericExec<TPgDirectFunc<&time_timetz>, true, false, TArgs_time_timetz_Policy>; }
struct TArgs_timestamp_cmp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_cmp_date() { return &GenericExec<TPgDirectFunc<&timestamp_cmp_date>, true, true, TArgs_timestamp_cmp_date_Policy>; }
struct TArgs_timestamp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_date() { return &GenericExec<TPgDirectFunc<&timestamp_date>, true, true, TArgs_timestamp_date_Policy>; }
struct TArgs_timestamp_eq_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_eq_date() { return &GenericExec<TPgDirectFunc<&timestamp_eq_date>, true, true, TArgs_timestamp_eq_date_Policy>; }
struct TArgs_timestamp_ge_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ge_date() { return &GenericExec<TPgDirectFunc<&timestamp_ge_date>, true, true, TArgs_timestamp_ge_date_Policy>; }
struct TArgs_timestamp_gt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_gt_date() { return &GenericExec<TPgDirectFunc<&timestamp_gt_date>, true, true, TArgs_timestamp_gt_date_Policy>; }
struct TArgs_timestamp_le_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_le_date() { return &GenericExec<TPgDirectFunc<&timestamp_le_date>, true, true, TArgs_timestamp_le_date_Policy>; }
struct TArgs_timestamp_lt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_lt_date() { return &GenericExec<TPgDirectFunc<&timestamp_lt_date>, true, true, TArgs_timestamp_lt_date_Policy>; }
struct TArgs_timestamp_ne_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ne_date() { return &GenericExec<TPgDirectFunc<&timestamp_ne_date>, true, true, TArgs_timestamp_ne_date_Policy>; }
struct TArgs_timestamp_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_time() { return &GenericExec<TPgDirectFunc<&timestamp_time>, true, true, TArgs_timestamp_time_Policy>; }
struct TArgs_timestamptz_cmp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_cmp_date() { return &GenericExec<TPgDirectFunc<&timestamptz_cmp_date>, true, true, TArgs_timestamptz_cmp_date_Policy>; }
struct TArgs_timestamptz_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_date() { return &GenericExec<TPgDirectFunc<&timestamptz_date>, true, true, TArgs_timestamptz_date_Policy>; }
struct TArgs_timestamptz_eq_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_eq_date() { return &GenericExec<TPgDirectFunc<&timestamptz_eq_date>, true, true, TArgs_timestamptz_eq_date_Policy>; }
struct TArgs_timestamptz_ge_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ge_date() { return &GenericExec<TPgDirectFunc<&timestamptz_ge_date>, true, true, TArgs_timestamptz_ge_date_Policy>; }
struct TArgs_timestamptz_gt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_gt_date() { return &GenericExec<TPgDirectFunc<&timestamptz_gt_date>, true, true, TArgs_timestamptz_gt_date_Policy>; }
struct TArgs_timestamptz_le_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_le_date() { return &GenericExec<TPgDirectFunc<&timestamptz_le_date>, true, true, TArgs_timestamptz_le_date_Policy>; }
struct TArgs_timestamptz_lt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_lt_date() { return &GenericExec<TPgDirectFunc<&timestamptz_lt_date>, true, true, TArgs_timestamptz_lt_date_Policy>; }
struct TArgs_timestamptz_ne_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ne_date() { return &GenericExec<TPgDirectFunc<&timestamptz_ne_date>, true, true, TArgs_timestamptz_ne_date_Policy>; }
struct TArgs_timestamptz_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_time() { return &GenericExec<TPgDirectFunc<&timestamptz_time>, true, true, TArgs_timestamptz_time_Policy>; }
struct TArgs_timestamptz_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_timetz() { return &GenericExec<TPgDirectFunc<&timestamptz_timetz>, true, false, TArgs_timestamptz_timetz_Policy>; }
struct TArgs_timetypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetypmodin() { return &GenericExec<TPgDirectFunc<&timetypmodin>, true, true, TArgs_timetypmodin_Policy>; }
struct TArgs_timetypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timetypmodout() { return &GenericExec<TPgDirectFunc<&timetypmodout>, true, false, TArgs_timetypmodout_Policy>; }
struct TArgs_timetz_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_cmp() { return &GenericExec<TPgDirectFunc<&timetz_cmp>, true, true, TArgs_timetz_cmp_Policy>; }
struct TArgs_timetz_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_eq() { return &GenericExec<TPgDirectFunc<&timetz_eq>, true, true, TArgs_timetz_eq_Policy>; }
struct TArgs_timetz_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_ge() { return &GenericExec<TPgDirectFunc<&timetz_ge>, true, true, TArgs_timetz_ge_Policy>; }
struct TArgs_timetz_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_gt() { return &GenericExec<TPgDirectFunc<&timetz_gt>, true, true, TArgs_timetz_gt_Policy>; }
struct TArgs_timetz_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_hash() { return &GenericExec<TPgDirectFunc<&timetz_hash>, true, true, TArgs_timetz_hash_Policy>; }
struct TArgs_timetz_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timetz_hash_extended() { return &GenericExec<TPgDirectFunc<&timetz_hash_extended>, true, true, TArgs_timetz_hash_extended_Policy>; }
struct TArgs_timetz_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timetz_in() { return &GenericExec<TPgDirectFunc<&timetz_in>, true, false, TArgs_timetz_in_Policy>; }
struct TArgs_timetz_izone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_izone() { return &GenericExec<TPgDirectFunc<&timetz_izone>, true, false, TArgs_timetz_izone_Policy>; }
struct TArgs_timetz_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_larger() { return &GenericExec<TPgDirectFunc<&timetz_larger>, true, false, TArgs_timetz_larger_Policy>; }
struct TArgs_timetz_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_le() { return &GenericExec<TPgDirectFunc<&timetz_le>, true, true, TArgs_timetz_le_Policy>; }
struct TArgs_timetz_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_lt() { return &GenericExec<TPgDirectFunc<&timetz_lt>, true, true, TArgs_timetz_lt_Policy>; }
struct TArgs_timetz_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_mi_interval() { return &GenericExec<TPgDirectFunc<&timetz_mi_interval>, true, false, TArgs_timetz_mi_interval_Policy>; }
struct TArgs_timetz_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_ne() { return &GenericExec<TPgDirectFunc<&timetz_ne>, true, true, TArgs_timetz_ne_Policy>; }
struct TArgs_timetz_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_out() { return &GenericExec<TPgDirectFunc<&timetz_out>, true, false, TArgs_timetz_out_Policy>; }
struct TArgs_timetz_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_part() { return &GenericExec<TPgDirectFunc<&timetz_part>, true, true, TArgs_timetz_part_Policy>; }
struct TArgs_timetz_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_pl_interval() { return &GenericExec<TPgDirectFunc<&timetz_pl_interval>, true, false, TArgs_timetz_pl_interval_Policy>; }
struct TArgs_timetz_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_timetz_recv() { return &GenericExec<TPgDirectFunc<&timetz_recv>, true, false, TArgs_timetz_recv_Policy>; }
struct TArgs_timetz_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timetz_scale() { return &GenericExec<TPgDirectFunc<&timetz_scale>, true, false, TArgs_timetz_scale_Policy>; }
struct TArgs_timetz_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_send() { return &GenericExec<TPgDirectFunc<&timetz_send>, true, false, TArgs_timetz_send_Policy>; }
struct TArgs_timetz_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_smaller() { return &GenericExec<TPgDirectFunc<&timetz_smaller>, true, false, TArgs_timetz_smaller_Policy>; }
struct TArgs_timetz_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_time() { return &GenericExec<TPgDirectFunc<&timetz_time>, true, true, TArgs_timetz_time_Policy>; }
struct TArgs_timetz_zone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_zone() { return &GenericExec<TPgDirectFunc<&timetz_zone>, true, false, TArgs_timetz_zone_Policy>; }
struct TArgs_timetztypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetztypmodin() { return &GenericExec<TPgDirectFunc<&timetztypmodin>, true, true, TArgs_timetztypmodin_Policy>; }
struct TArgs_timetztypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timetztypmodout() { return &GenericExec<TPgDirectFunc<&timetztypmodout>, true, false, TArgs_timetztypmodout_Policy>; }
struct TArgs_to_ascii_default_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_ascii_default() { return &GenericExec<TPgDirectFunc<&to_ascii_default>, true, false, TArgs_to_ascii_default_Policy>; }
struct TArgs_to_ascii_enc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_to_ascii_enc() { return &GenericExec<TPgDirectFunc<&to_ascii_enc>, true, false, TArgs_to_ascii_enc_Policy>; }
struct TArgs_to_ascii_encname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_to_ascii_encname() { return &GenericExec<TPgDirectFunc<&to_ascii_encname>, true, false, TArgs_to_ascii_encname_Policy>; }
struct TArgs_ts_headline_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline() { return &GenericExec<TPgDirectFunc<&ts_headline>, true, false, TArgs_ts_headline_Policy>; }
struct TArgs_ts_headline_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_byid() { return &GenericExec<TPgDirectFunc<&ts_headline_byid>, true, false, TArgs_ts_headline_byid_Policy>; }
struct TArgs_ts_headline_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_byid_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_byid_opt>, true, false, TArgs_ts_headline_byid_opt_Policy>; }
struct TArgs_ts_headline_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline_json() { return &GenericExec<TPgDirectFunc<&ts_headline_json>, true, false, TArgs_ts_headline_json_Policy>; }
struct TArgs_ts_headline_json_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_json_byid() { return &GenericExec<TPgDirectFunc<&ts_headline_json_byid>, true, false, TArgs_ts_headline_json_byid_Policy>; }
struct TArgs_ts_headline_json_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_json_byid_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_json_byid_opt>, true, false, TArgs_ts_headline_json_byid_opt_Policy>; }
struct TArgs_ts_headline_json_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_json_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_json_opt>, true, false, TArgs_ts_headline_json_opt_Policy>; }
struct TArgs_ts_headline_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline_jsonb() { return &GenericExec<TPgDirectFunc<&ts_headline_jsonb>, true, false, TArgs_ts_headline_jsonb_Policy>; }
struct TArgs_ts_headline_jsonb_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_jsonb_byid() { return &GenericExec<TPgDirectFunc<&ts_headline_jsonb_byid>, true, false, TArgs_ts_headline_jsonb_byid_Policy>; }
struct TArgs_ts_headline_jsonb_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_jsonb_byid_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_jsonb_byid_opt>, true, false, TArgs_ts_headline_jsonb_byid_opt_Policy>; }
struct TArgs_ts_headline_jsonb_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_jsonb_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_jsonb_opt>, true, false, TArgs_ts_headline_jsonb_opt_Policy>; }
struct TArgs_ts_headline_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_opt() { return &GenericExec<TPgDirectFunc<&ts_headline_opt>, true, false, TArgs_ts_headline_opt_Policy>; }
struct TArgs_tsm_bernoulli_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_bernoulli_handler() { return &GenericExec<TPgDirectFunc<&tsm_bernoulli_handler>, true, true, TArgs_tsm_bernoulli_handler_Policy>; }
struct TArgs_tsrange_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_tsrange_subdiff() { return &GenericExec<TPgDirectFunc<&tsrange_subdiff>, true, true, TArgs_tsrange_subdiff_Policy>; }
struct TArgs_tstzrange_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_tstzrange_subdiff() { return &GenericExec<TPgDirectFunc<&tstzrange_subdiff>, true, true, TArgs_tstzrange_subdiff_Policy>; }
struct TArgs_varchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varchar() { return &GenericExec<TPgDirectFunc<&varchar>, true, false, TArgs_varchar_Policy>; }
struct TArgs_varchar_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varchar_support() { return &GenericExec<TPgDirectFunc<&varchar_support>, true, true, TArgs_varchar_support_Policy>; }
struct TArgs_varcharin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varcharin() { return &GenericExec<TPgDirectFunc<&varcharin>, true, false, TArgs_varcharin_Policy>; }
struct TArgs_varcharout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varcharout() { return &GenericExec<TPgDirectFunc<&varcharout>, true, false, TArgs_varcharout_Policy>; }
struct TArgs_varcharrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_varcharrecv() { return &GenericExec<TPgDirectFunc<&varcharrecv>, true, false, TArgs_varcharrecv_Policy>; }
struct TArgs_varcharsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varcharsend() { return &GenericExec<TPgDirectFunc<&varcharsend>, true, false, TArgs_varcharsend_Policy>; }
struct TArgs_varchartypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varchartypmodin() { return &GenericExec<TPgDirectFunc<&varchartypmodin>, true, true, TArgs_varchartypmodin_Policy>; }
struct TArgs_varchartypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varchartypmodout() { return &GenericExec<TPgDirectFunc<&varchartypmodout>, true, false, TArgs_varchartypmodout_Policy>; }
struct TArgs_xid8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8cmp() { return &GenericExec<TPgDirectFunc<&xid8cmp>, true, true, TArgs_xid8cmp_Policy>; }
struct TArgs_xid8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8eq() { return &GenericExec<TPgDirectFunc<&xid8eq>, true, true, TArgs_xid8eq_Policy>; }
struct TArgs_xid8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8ge() { return &GenericExec<TPgDirectFunc<&xid8ge>, true, true, TArgs_xid8ge_Policy>; }
struct TArgs_xid8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8gt() { return &GenericExec<TPgDirectFunc<&xid8gt>, true, true, TArgs_xid8gt_Policy>; }
struct TArgs_xid8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xid8in() { return &GenericExec<TPgDirectFunc<&xid8in>, true, true, TArgs_xid8in_Policy>; }
struct TArgs_xid8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8le() { return &GenericExec<TPgDirectFunc<&xid8le>, true, true, TArgs_xid8le_Policy>; }
struct TArgs_xid8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8lt() { return &GenericExec<TPgDirectFunc<&xid8lt>, true, true, TArgs_xid8lt_Policy>; }
struct TArgs_xid8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8ne() { return &GenericExec<TPgDirectFunc<&xid8ne>, true, true, TArgs_xid8ne_Policy>; }
struct TArgs_xid8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8out() { return &GenericExec<TPgDirectFunc<&xid8out>, true, false, TArgs_xid8out_Policy>; }
struct TArgs_xid8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8recv() { return &GenericExec<TPgDirectFunc<&xid8recv>, true, true, TArgs_xid8recv_Policy>; }
struct TArgs_xid8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8send() { return &GenericExec<TPgDirectFunc<&xid8send>, true, false, TArgs_xid8send_Policy>; }
struct TArgs_xid8toxid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8toxid() { return &GenericExec<TPgDirectFunc<&xid8toxid>, true, true, TArgs_xid8toxid_Policy>; }
struct TArgs_xid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid_age() { return &GenericExec<TPgDirectFunc<&xid_age>, true, true, TArgs_xid_age_Policy>; }
struct TArgs_xideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xideq() { return &GenericExec<TPgDirectFunc<&xideq>, true, true, TArgs_xideq_Policy>; }
struct TArgs_xidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xidin() { return &GenericExec<TPgDirectFunc<&xidin>, true, true, TArgs_xidin_Policy>; }
struct TArgs_xidneq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xidneq() { return &GenericExec<TPgDirectFunc<&xidneq>, true, true, TArgs_xidneq_Policy>; }
struct TArgs_xidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidout() { return &GenericExec<TPgDirectFunc<&xidout>, true, false, TArgs_xidout_Policy>; }
struct TArgs_xidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidrecv() { return &GenericExec<TPgDirectFunc<&xidrecv>, true, true, TArgs_xidrecv_Policy>; }
struct TArgs_xidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidsend() { return &GenericExec<TPgDirectFunc<&xidsend>, true, false, TArgs_xidsend_Policy>; }
struct TArgs_xml_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_in() { return &GenericExec<TPgDirectFunc<&xml_in>, true, false, TArgs_xml_in_Policy>; }
struct TArgs_xml_is_well_formed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed() { return &GenericExec<TPgDirectFunc<&xml_is_well_formed>, true, true, TArgs_xml_is_well_formed_Policy>; }
struct TArgs_xml_is_well_formed_content_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed_content() { return &GenericExec<TPgDirectFunc<&xml_is_well_formed_content>, true, true, TArgs_xml_is_well_formed_content_Policy>; }
struct TArgs_xml_is_well_formed_document_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed_document() { return &GenericExec<TPgDirectFunc<&xml_is_well_formed_document>, true, true, TArgs_xml_is_well_formed_document_Policy>; }
struct TArgs_xml_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_out() { return &GenericExec<TPgDirectFunc<&xml_out>, true, false, TArgs_xml_out_Policy>; }
struct TArgs_xml_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xml_recv() { return &GenericExec<TPgDirectFunc<&xml_recv>, true, false, TArgs_xml_recv_Policy>; }
struct TArgs_xml_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_send() { return &GenericExec<TPgDirectFunc<&xml_send>, true, false, TArgs_xml_send_Policy>; }
struct TArgs_xmlcomment_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xmlcomment() { return &GenericExec<TPgDirectFunc<&xmlcomment>, true, false, TArgs_xmlcomment_Policy>; }
struct TArgs_xmlconcat2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlconcat2() { return &GenericExec<TPgDirectFunc<&xmlconcat2>, false, false, TArgs_xmlconcat2_Policy>; }
struct TArgs_xmlexists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlexists() { return &GenericExec<TPgDirectFunc<&xmlexists>, true, true, TArgs_xmlexists_Policy>; }
struct TArgs_xmltotext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xmltotext() { return &GenericExec<TPgDirectFunc<&xmltotext>, true, false, TArgs_xmltotext_Policy>; }
struct TArgs_xmlvalidate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlvalidate() { return &GenericExec<TPgDirectFunc<&xmlvalidate>, true, true, TArgs_xmlvalidate_Policy>; }
struct TArgs_xpath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_xpath() { return &GenericExec<TPgDirectFunc<&xpath>, true, false, TArgs_xpath_Policy>; }
struct TArgs_xpath_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_xpath_exists() { return &GenericExec<TPgDirectFunc<&xpath_exists>, true, true, TArgs_xpath_exists_Policy>; }
