struct TArgs_array_subscript_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_subscript_handler() { return &GenericExec<TPgDirectFunc<&array_subscript_handler>, true, true, TArgs_array_subscript_handler_Policy>; }
struct TArgs_array_to_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_to_text() { return &GenericExec<TPgDirectFunc<&array_to_text>, true, false, TArgs_array_to_text_Policy>; }
struct TArgs_array_to_text_null_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_array_to_text_null() { return &GenericExec<TPgDirectFunc<&array_to_text_null>, false, false, TArgs_array_to_text_null_Policy>; }
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
struct TArgs_brin_inclusion_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_inclusion_add_value() { return &GenericExec<TPgDirectFunc<&brin_inclusion_add_value>, true, true, TArgs_brin_inclusion_add_value_Policy>; }
struct TArgs_brin_inclusion_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_inclusion_consistent() { return &GenericExec<TPgDirectFunc<&brin_inclusion_consistent>, true, true, TArgs_brin_inclusion_consistent_Policy>; }
struct TArgs_brin_inclusion_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_inclusion_opcinfo() { return &GenericExec<TPgDirectFunc<&brin_inclusion_opcinfo>, true, true, TArgs_brin_inclusion_opcinfo_Policy>; }
struct TArgs_brin_inclusion_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_inclusion_union() { return &GenericExec<TPgDirectFunc<&brin_inclusion_union>, true, true, TArgs_brin_inclusion_union_Policy>; }
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
struct TArgs_btequalimage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btequalimage() { return &GenericExec<TPgDirectFunc<&btequalimage>, true, true, TArgs_btequalimage_Policy>; }
struct TArgs_btfloat48cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat48cmp() { return &GenericExec<TPgDirectFunc<&btfloat48cmp>, true, true, TArgs_btfloat48cmp_Policy>; }
struct TArgs_btfloat4cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat4cmp() { return &GenericExec<TPgDirectFunc<&btfloat4cmp>, true, true, TArgs_btfloat4cmp_Policy>; }
struct TArgs_btfloat4sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btfloat4sortsupport() { return &GenericExec<TPgDirectFunc<&btfloat4sortsupport>, true, true, TArgs_btfloat4sortsupport_Policy>; }
struct TArgs_btfloat84cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat84cmp() { return &GenericExec<TPgDirectFunc<&btfloat84cmp>, true, true, TArgs_btfloat84cmp_Policy>; }
struct TArgs_btfloat8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat8cmp() { return &GenericExec<TPgDirectFunc<&btfloat8cmp>, true, true, TArgs_btfloat8cmp_Policy>; }
struct TArgs_btfloat8sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btfloat8sortsupport() { return &GenericExec<TPgDirectFunc<&btfloat8sortsupport>, true, true, TArgs_btfloat8sortsupport_Policy>; }
struct TArgs_bthandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bthandler() { return &GenericExec<TPgDirectFunc<&bthandler>, true, true, TArgs_bthandler_Policy>; }
struct TArgs_btnametextcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btnametextcmp() { return &GenericExec<TPgDirectFunc<&btnametextcmp>, true, true, TArgs_btnametextcmp_Policy>; }
struct TArgs_bttext_pattern_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttext_pattern_cmp() { return &GenericExec<TPgDirectFunc<&bttext_pattern_cmp>, true, true, TArgs_bttext_pattern_cmp_Policy>; }
struct TArgs_bttext_pattern_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bttext_pattern_sortsupport() { return &GenericExec<TPgDirectFunc<&bttext_pattern_sortsupport>, true, true, TArgs_bttext_pattern_sortsupport_Policy>; }
struct TArgs_bttextcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttextcmp() { return &GenericExec<TPgDirectFunc<&bttextcmp>, true, true, TArgs_bttextcmp_Policy>; }
struct TArgs_bttextnamecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttextnamecmp() { return &GenericExec<TPgDirectFunc<&bttextnamecmp>, true, true, TArgs_bttextnamecmp_Policy>; }
struct TArgs_bttextsortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bttextsortsupport() { return &GenericExec<TPgDirectFunc<&bttextsortsupport>, true, true, TArgs_bttextsortsupport_Policy>; }
struct TArgs_btvarstrequalimage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btvarstrequalimage() { return &GenericExec<TPgDirectFunc<&btvarstrequalimage>, true, true, TArgs_btvarstrequalimage_Policy>; }
struct TArgs_byteaGetBit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_byteaGetBit() { return &GenericExec<TPgDirectFunc<&byteaGetBit>, true, true, TArgs_byteaGetBit_Policy>; }
struct TArgs_byteaGetByte_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_byteaGetByte() { return &GenericExec<TPgDirectFunc<&byteaGetByte>, true, true, TArgs_byteaGetByte_Policy>; }
struct TArgs_byteaSetBit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_byteaSetBit() { return &GenericExec<TPgDirectFunc<&byteaSetBit>, true, false, TArgs_byteaSetBit_Policy>; }
struct TArgs_byteaSetByte_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_byteaSetByte() { return &GenericExec<TPgDirectFunc<&byteaSetByte>, true, false, TArgs_byteaSetByte_Policy>; }
struct TArgs_bytea_bit_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bytea_bit_count() { return &GenericExec<TPgDirectFunc<&bytea_bit_count>, true, true, TArgs_bytea_bit_count_Policy>; }
struct TArgs_bytea_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytea_sortsupport() { return &GenericExec<TPgDirectFunc<&bytea_sortsupport>, true, true, TArgs_bytea_sortsupport_Policy>; }
struct TArgs_bytea_string_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytea_string_agg_finalfn() { return &GenericExec<TPgDirectFunc<&bytea_string_agg_finalfn>, false, false, TArgs_bytea_string_agg_finalfn_Policy>; }
struct TArgs_bytea_string_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_bytea_string_agg_transfn() { return &GenericExec<TPgDirectFunc<&bytea_string_agg_transfn>, false, true, TArgs_bytea_string_agg_transfn_Policy>; }
struct TArgs_bytea_substr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bytea_substr() { return &GenericExec<TPgDirectFunc<&bytea_substr>, true, false, TArgs_bytea_substr_Policy>; }
struct TArgs_bytea_substr_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bytea_substr_no_len() { return &GenericExec<TPgDirectFunc<&bytea_substr_no_len>, true, false, TArgs_bytea_substr_no_len_Policy>; }
struct TArgs_byteacat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteacat() { return &GenericExec<TPgDirectFunc<&byteacat>, true, false, TArgs_byteacat_Policy>; }
struct TArgs_byteacmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteacmp() { return &GenericExec<TPgDirectFunc<&byteacmp>, true, true, TArgs_byteacmp_Policy>; }
struct TArgs_byteaeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteaeq() { return &GenericExec<TPgDirectFunc<&byteaeq>, true, true, TArgs_byteaeq_Policy>; }
struct TArgs_byteage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteage() { return &GenericExec<TPgDirectFunc<&byteage>, true, true, TArgs_byteage_Policy>; }
struct TArgs_byteagt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteagt() { return &GenericExec<TPgDirectFunc<&byteagt>, true, true, TArgs_byteagt_Policy>; }
struct TArgs_byteain_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteain() { return &GenericExec<TPgDirectFunc<&byteain>, true, false, TArgs_byteain_Policy>; }
struct TArgs_byteale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteale() { return &GenericExec<TPgDirectFunc<&byteale>, true, true, TArgs_byteale_Policy>; }
struct TArgs_bytealt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealt() { return &GenericExec<TPgDirectFunc<&bytealt>, true, true, TArgs_bytealt_Policy>; }
struct TArgs_byteane_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteane() { return &GenericExec<TPgDirectFunc<&byteane>, true, true, TArgs_byteane_Policy>; }
struct TArgs_byteaoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteaoctetlen() { return &GenericExec<TPgDirectFunc<&byteaoctetlen>, true, true, TArgs_byteaoctetlen_Policy>; }
struct TArgs_byteaout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteaout() { return &GenericExec<TPgDirectFunc<&byteaout>, true, false, TArgs_byteaout_Policy>; }
struct TArgs_byteaoverlay_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_byteaoverlay() { return &GenericExec<TPgDirectFunc<&byteaoverlay>, true, false, TArgs_byteaoverlay_Policy>; }
struct TArgs_byteaoverlay_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_byteaoverlay_no_len() { return &GenericExec<TPgDirectFunc<&byteaoverlay_no_len>, true, false, TArgs_byteaoverlay_no_len_Policy>; }
struct TArgs_byteapos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteapos() { return &GenericExec<TPgDirectFunc<&byteapos>, true, true, TArgs_byteapos_Policy>; }
struct TArgs_bytearecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytearecv() { return &GenericExec<TPgDirectFunc<&bytearecv>, true, false, TArgs_bytearecv_Policy>; }
struct TArgs_byteasend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteasend() { return &GenericExec<TPgDirectFunc<&byteasend>, true, false, TArgs_byteasend_Policy>; }
struct TArgs_cash_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_cmp() { return &GenericExec<TPgDirectFunc<&cash_cmp>, true, true, TArgs_cash_cmp_Policy>; }
struct TArgs_cash_div_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_cash() { return &GenericExec<TPgDirectFunc<&cash_div_cash>, true, true, TArgs_cash_div_cash_Policy>; }
struct TArgs_cash_div_flt4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_flt4() { return &GenericExec<TPgDirectFunc<&cash_div_flt4>, true, true, TArgs_cash_div_flt4_Policy>; }
struct TArgs_cash_div_flt8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_flt8() { return &GenericExec<TPgDirectFunc<&cash_div_flt8>, true, true, TArgs_cash_div_flt8_Policy>; }
struct TArgs_cash_div_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_int2() { return &GenericExec<TPgDirectFunc<&cash_div_int2>, true, true, TArgs_cash_div_int2_Policy>; }
struct TArgs_cash_div_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_int4() { return &GenericExec<TPgDirectFunc<&cash_div_int4>, true, true, TArgs_cash_div_int4_Policy>; }
struct TArgs_cash_div_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_div_int8() { return &GenericExec<TPgDirectFunc<&cash_div_int8>, true, true, TArgs_cash_div_int8_Policy>; }
struct TArgs_cash_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_eq() { return &GenericExec<TPgDirectFunc<&cash_eq>, true, true, TArgs_cash_eq_Policy>; }
struct TArgs_cash_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_ge() { return &GenericExec<TPgDirectFunc<&cash_ge>, true, true, TArgs_cash_ge_Policy>; }
struct TArgs_cash_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_gt() { return &GenericExec<TPgDirectFunc<&cash_gt>, true, true, TArgs_cash_gt_Policy>; }
struct TArgs_cash_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cash_in() { return &GenericExec<TPgDirectFunc<&cash_in>, true, true, TArgs_cash_in_Policy>; }
struct TArgs_cash_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_le() { return &GenericExec<TPgDirectFunc<&cash_le>, true, true, TArgs_cash_le_Policy>; }
struct TArgs_cash_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_lt() { return &GenericExec<TPgDirectFunc<&cash_lt>, true, true, TArgs_cash_lt_Policy>; }
struct TArgs_cash_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mi() { return &GenericExec<TPgDirectFunc<&cash_mi>, true, true, TArgs_cash_mi_Policy>; }
struct TArgs_cash_mul_flt4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mul_flt4() { return &GenericExec<TPgDirectFunc<&cash_mul_flt4>, true, true, TArgs_cash_mul_flt4_Policy>; }
struct TArgs_cash_mul_flt8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mul_flt8() { return &GenericExec<TPgDirectFunc<&cash_mul_flt8>, true, true, TArgs_cash_mul_flt8_Policy>; }
struct TArgs_cash_mul_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mul_int2() { return &GenericExec<TPgDirectFunc<&cash_mul_int2>, true, true, TArgs_cash_mul_int2_Policy>; }
struct TArgs_cash_mul_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mul_int4() { return &GenericExec<TPgDirectFunc<&cash_mul_int4>, true, true, TArgs_cash_mul_int4_Policy>; }
struct TArgs_cash_mul_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_mul_int8() { return &GenericExec<TPgDirectFunc<&cash_mul_int8>, true, true, TArgs_cash_mul_int8_Policy>; }
struct TArgs_cash_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_ne() { return &GenericExec<TPgDirectFunc<&cash_ne>, true, true, TArgs_cash_ne_Policy>; }
struct TArgs_cash_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cash_numeric() { return &GenericExec<TPgDirectFunc<&cash_numeric>, true, false, TArgs_cash_numeric_Policy>; }
struct TArgs_cash_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cash_out() { return &GenericExec<TPgDirectFunc<&cash_out>, true, false, TArgs_cash_out_Policy>; }
struct TArgs_cash_pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cash_pl() { return &GenericExec<TPgDirectFunc<&cash_pl>, true, true, TArgs_cash_pl_Policy>; }
struct TArgs_cash_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cash_recv() { return &GenericExec<TPgDirectFunc<&cash_recv>, true, true, TArgs_cash_recv_Policy>; }
struct TArgs_cash_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cash_send() { return &GenericExec<TPgDirectFunc<&cash_send>, true, false, TArgs_cash_send_Policy>; }
struct TArgs_cash_words_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cash_words() { return &GenericExec<TPgDirectFunc<&cash_words>, true, false, TArgs_cash_words_Policy>; }
struct TArgs_cashlarger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cashlarger() { return &GenericExec<TPgDirectFunc<&cashlarger>, true, true, TArgs_cashlarger_Policy>; }
struct TArgs_cashsmaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cashsmaller() { return &GenericExec<TPgDirectFunc<&cashsmaller>, true, true, TArgs_cashsmaller_Policy>; }
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
struct TArgs_dacos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacos() { return &GenericExec<TPgDirectFunc<&dacos>, true, true, TArgs_dacos_Policy>; }
struct TArgs_dacosd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacosd() { return &GenericExec<TPgDirectFunc<&dacosd>, true, true, TArgs_dacosd_Policy>; }
struct TArgs_dacosh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacosh() { return &GenericExec<TPgDirectFunc<&dacosh>, true, true, TArgs_dacosh_Policy>; }
struct TArgs_dasin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasin() { return &GenericExec<TPgDirectFunc<&dasin>, true, true, TArgs_dasin_Policy>; }
struct TArgs_dasind_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasind() { return &GenericExec<TPgDirectFunc<&dasind>, true, true, TArgs_dasind_Policy>; }
struct TArgs_dasinh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasinh() { return &GenericExec<TPgDirectFunc<&dasinh>, true, true, TArgs_dasinh_Policy>; }
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
struct TArgs_datan_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datan() { return &GenericExec<TPgDirectFunc<&datan>, true, true, TArgs_datan_Policy>; }
struct TArgs_datan2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datan2() { return &GenericExec<TPgDirectFunc<&datan2>, true, true, TArgs_datan2_Policy>; }
struct TArgs_datan2d_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datan2d() { return &GenericExec<TPgDirectFunc<&datan2d>, true, true, TArgs_datan2d_Policy>; }
struct TArgs_datand_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datand() { return &GenericExec<TPgDirectFunc<&datand>, true, true, TArgs_datand_Policy>; }
struct TArgs_datanh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datanh() { return &GenericExec<TPgDirectFunc<&datanh>, true, true, TArgs_datanh_Policy>; }
struct TArgs_dcbrt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcbrt() { return &GenericExec<TPgDirectFunc<&dcbrt>, true, true, TArgs_dcbrt_Policy>; }
struct TArgs_dceil_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dceil() { return &GenericExec<TPgDirectFunc<&dceil>, true, true, TArgs_dceil_Policy>; }
struct TArgs_dcos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcos() { return &GenericExec<TPgDirectFunc<&dcos>, true, true, TArgs_dcos_Policy>; }
struct TArgs_dcosd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcosd() { return &GenericExec<TPgDirectFunc<&dcosd>, true, true, TArgs_dcosd_Policy>; }
struct TArgs_dcosh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcosh() { return &GenericExec<TPgDirectFunc<&dcosh>, true, true, TArgs_dcosh_Policy>; }
struct TArgs_dcot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcot() { return &GenericExec<TPgDirectFunc<&dcot>, true, true, TArgs_dcot_Policy>; }
struct TArgs_dcotd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcotd() { return &GenericExec<TPgDirectFunc<&dcotd>, true, true, TArgs_dcotd_Policy>; }
struct TArgs_degrees_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_degrees() { return &GenericExec<TPgDirectFunc<&degrees>, true, true, TArgs_degrees_Policy>; }
struct TArgs_dexp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dexp() { return &GenericExec<TPgDirectFunc<&dexp>, true, true, TArgs_dexp_Policy>; }
struct TArgs_dfloor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dfloor() { return &GenericExec<TPgDirectFunc<&dfloor>, true, true, TArgs_dfloor_Policy>; }
struct TArgs_dlog1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dlog1() { return &GenericExec<TPgDirectFunc<&dlog1>, true, true, TArgs_dlog1_Policy>; }
struct TArgs_dlog10_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dlog10() { return &GenericExec<TPgDirectFunc<&dlog10>, true, true, TArgs_dlog10_Policy>; }
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
struct TArgs_dpi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_dpi() { return &GenericExec<TPgDirectFunc<&dpi>, true, true, TArgs_dpi_Policy>; }
struct TArgs_dpow_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_dpow() { return &GenericExec<TPgDirectFunc<&dpow>, true, true, TArgs_dpow_Policy>; }
struct TArgs_drandom_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_drandom() { return &GenericExec<TPgDirectFunc<&drandom>, true, true, TArgs_drandom_Policy>; }
struct TArgs_dround_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dround() { return &GenericExec<TPgDirectFunc<&dround>, true, true, TArgs_dround_Policy>; }
struct TArgs_dsign_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsign() { return &GenericExec<TPgDirectFunc<&dsign>, true, true, TArgs_dsign_Policy>; }
struct TArgs_dsin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsin() { return &GenericExec<TPgDirectFunc<&dsin>, true, true, TArgs_dsin_Policy>; }
struct TArgs_dsind_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsind() { return &GenericExec<TPgDirectFunc<&dsind>, true, true, TArgs_dsind_Policy>; }
struct TArgs_dsinh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsinh() { return &GenericExec<TPgDirectFunc<&dsinh>, true, true, TArgs_dsinh_Policy>; }
struct TArgs_dsqrt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsqrt() { return &GenericExec<TPgDirectFunc<&dsqrt>, true, true, TArgs_dsqrt_Policy>; }
struct TArgs_dtan_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtan() { return &GenericExec<TPgDirectFunc<&dtan>, true, true, TArgs_dtan_Policy>; }
struct TArgs_dtand_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtand() { return &GenericExec<TPgDirectFunc<&dtand>, true, true, TArgs_dtand_Policy>; }
struct TArgs_dtanh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtanh() { return &GenericExec<TPgDirectFunc<&dtanh>, true, true, TArgs_dtanh_Policy>; }
struct TArgs_dtof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtof() { return &GenericExec<TPgDirectFunc<&dtof>, true, true, TArgs_dtof_Policy>; }
struct TArgs_dtoi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi2() { return &GenericExec<TPgDirectFunc<&dtoi2>, true, true, TArgs_dtoi2_Policy>; }
struct TArgs_dtoi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi4() { return &GenericExec<TPgDirectFunc<&dtoi4>, true, true, TArgs_dtoi4_Policy>; }
struct TArgs_dtrunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtrunc() { return &GenericExec<TPgDirectFunc<&dtrunc>, true, true, TArgs_dtrunc_Policy>; }
struct TArgs_elem_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_elem_contained_by_multirange() { return &GenericExec<TPgDirectFunc<&elem_contained_by_multirange>, true, true, TArgs_elem_contained_by_multirange_Policy>; }
struct TArgs_float48div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48div() { return &GenericExec<TPgDirectFunc<&float48div>, true, true, TArgs_float48div_Policy>; }
struct TArgs_float48eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48eq() { return &GenericExec<TPgDirectFunc<&float48eq>, true, true, TArgs_float48eq_Policy>; }
struct TArgs_float48ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48ge() { return &GenericExec<TPgDirectFunc<&float48ge>, true, true, TArgs_float48ge_Policy>; }
struct TArgs_float48gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48gt() { return &GenericExec<TPgDirectFunc<&float48gt>, true, true, TArgs_float48gt_Policy>; }
struct TArgs_float48le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48le() { return &GenericExec<TPgDirectFunc<&float48le>, true, true, TArgs_float48le_Policy>; }
struct TArgs_float48lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48lt() { return &GenericExec<TPgDirectFunc<&float48lt>, true, true, TArgs_float48lt_Policy>; }
struct TArgs_float48mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48mi() { return &GenericExec<TPgDirectFunc<&float48mi>, true, true, TArgs_float48mi_Policy>; }
struct TArgs_float48mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48mul() { return &GenericExec<TPgDirectFunc<&float48mul>, true, true, TArgs_float48mul_Policy>; }
struct TArgs_float48ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48ne() { return &GenericExec<TPgDirectFunc<&float48ne>, true, true, TArgs_float48ne_Policy>; }
struct TArgs_float48pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48pl() { return &GenericExec<TPgDirectFunc<&float48pl>, true, true, TArgs_float48pl_Policy>; }
struct TArgs_float4_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_float4_accum() { return &GenericExec<TPgDirectFunc<&float4_accum>, true, false, TArgs_float4_accum_Policy>; }
struct TArgs_float4abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4abs() { return &GenericExec<TPgDirectFunc<&float4abs>, true, true, TArgs_float4abs_Policy>; }
struct TArgs_float4div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4div() { return &GenericExec<TPgDirectFunc<&float4div>, true, true, TArgs_float4div_Policy>; }
struct TArgs_float4eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4eq() { return &GenericExec<TPgDirectFunc<&float4eq>, true, true, TArgs_float4eq_Policy>; }
struct TArgs_float4ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4ge() { return &GenericExec<TPgDirectFunc<&float4ge>, true, true, TArgs_float4ge_Policy>; }
struct TArgs_float4gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4gt() { return &GenericExec<TPgDirectFunc<&float4gt>, true, true, TArgs_float4gt_Policy>; }
struct TArgs_float4in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float4in() { return &GenericExec<TPgDirectFunc<&float4in>, true, true, TArgs_float4in_Policy>; }
struct TArgs_float4larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4larger() { return &GenericExec<TPgDirectFunc<&float4larger>, true, true, TArgs_float4larger_Policy>; }
struct TArgs_float4le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4le() { return &GenericExec<TPgDirectFunc<&float4le>, true, true, TArgs_float4le_Policy>; }
struct TArgs_float4lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4lt() { return &GenericExec<TPgDirectFunc<&float4lt>, true, true, TArgs_float4lt_Policy>; }
struct TArgs_float4mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4mi() { return &GenericExec<TPgDirectFunc<&float4mi>, true, true, TArgs_float4mi_Policy>; }
struct TArgs_float4mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4mul() { return &GenericExec<TPgDirectFunc<&float4mul>, true, true, TArgs_float4mul_Policy>; }
struct TArgs_float4ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4ne() { return &GenericExec<TPgDirectFunc<&float4ne>, true, true, TArgs_float4ne_Policy>; }
struct TArgs_float4out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4out() { return &GenericExec<TPgDirectFunc<&float4out>, true, false, TArgs_float4out_Policy>; }
struct TArgs_float4pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4pl() { return &GenericExec<TPgDirectFunc<&float4pl>, true, true, TArgs_float4pl_Policy>; }
struct TArgs_float4recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4recv() { return &GenericExec<TPgDirectFunc<&float4recv>, true, true, TArgs_float4recv_Policy>; }
struct TArgs_float4send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4send() { return &GenericExec<TPgDirectFunc<&float4send>, true, false, TArgs_float4send_Policy>; }
struct TArgs_float4smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4smaller() { return &GenericExec<TPgDirectFunc<&float4smaller>, true, true, TArgs_float4smaller_Policy>; }
struct TArgs_float4um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4um() { return &GenericExec<TPgDirectFunc<&float4um>, true, true, TArgs_float4um_Policy>; }
struct TArgs_float4up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4up() { return &GenericExec<TPgDirectFunc<&float4up>, true, true, TArgs_float4up_Policy>; }
struct TArgs_float84div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84div() { return &GenericExec<TPgDirectFunc<&float84div>, true, true, TArgs_float84div_Policy>; }
struct TArgs_float84eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84eq() { return &GenericExec<TPgDirectFunc<&float84eq>, true, true, TArgs_float84eq_Policy>; }
struct TArgs_float84ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84ge() { return &GenericExec<TPgDirectFunc<&float84ge>, true, true, TArgs_float84ge_Policy>; }
struct TArgs_float84gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84gt() { return &GenericExec<TPgDirectFunc<&float84gt>, true, true, TArgs_float84gt_Policy>; }
struct TArgs_float84le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84le() { return &GenericExec<TPgDirectFunc<&float84le>, true, true, TArgs_float84le_Policy>; }
struct TArgs_float84lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84lt() { return &GenericExec<TPgDirectFunc<&float84lt>, true, true, TArgs_float84lt_Policy>; }
struct TArgs_float84mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84mi() { return &GenericExec<TPgDirectFunc<&float84mi>, true, true, TArgs_float84mi_Policy>; }
struct TArgs_float84mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84mul() { return &GenericExec<TPgDirectFunc<&float84mul>, true, true, TArgs_float84mul_Policy>; }
struct TArgs_float84ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84ne() { return &GenericExec<TPgDirectFunc<&float84ne>, true, true, TArgs_float84ne_Policy>; }
struct TArgs_float84pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84pl() { return &GenericExec<TPgDirectFunc<&float84pl>, true, true, TArgs_float84pl_Policy>; }
struct TArgs_float8_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_float8_accum() { return &GenericExec<TPgDirectFunc<&float8_accum>, true, false, TArgs_float8_accum_Policy>; }
struct TArgs_float8_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_avg() { return &GenericExec<TPgDirectFunc<&float8_avg>, true, true, TArgs_float8_avg_Policy>; }
struct TArgs_float8_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_float8_combine() { return &GenericExec<TPgDirectFunc<&float8_combine>, true, false, TArgs_float8_combine_Policy>; }
struct TArgs_float8_corr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_corr() { return &GenericExec<TPgDirectFunc<&float8_corr>, true, true, TArgs_float8_corr_Policy>; }
struct TArgs_float8_covar_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_covar_pop() { return &GenericExec<TPgDirectFunc<&float8_covar_pop>, true, true, TArgs_float8_covar_pop_Policy>; }
struct TArgs_float8_covar_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_covar_samp() { return &GenericExec<TPgDirectFunc<&float8_covar_samp>, true, true, TArgs_float8_covar_samp_Policy>; }
struct TArgs_float8_regr_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_float8_regr_accum() { return &GenericExec<TPgDirectFunc<&float8_regr_accum>, true, false, TArgs_float8_regr_accum_Policy>; }
struct TArgs_float8_regr_avgx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_avgx() { return &GenericExec<TPgDirectFunc<&float8_regr_avgx>, true, true, TArgs_float8_regr_avgx_Policy>; }
struct TArgs_float8_regr_avgy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_avgy() { return &GenericExec<TPgDirectFunc<&float8_regr_avgy>, true, true, TArgs_float8_regr_avgy_Policy>; }
struct TArgs_float8_regr_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_float8_regr_combine() { return &GenericExec<TPgDirectFunc<&float8_regr_combine>, true, false, TArgs_float8_regr_combine_Policy>; }
struct TArgs_float8_regr_intercept_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_intercept() { return &GenericExec<TPgDirectFunc<&float8_regr_intercept>, true, true, TArgs_float8_regr_intercept_Policy>; }
struct TArgs_float8_regr_r2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_r2() { return &GenericExec<TPgDirectFunc<&float8_regr_r2>, true, true, TArgs_float8_regr_r2_Policy>; }
struct TArgs_float8_regr_slope_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_slope() { return &GenericExec<TPgDirectFunc<&float8_regr_slope>, true, true, TArgs_float8_regr_slope_Policy>; }
struct TArgs_float8_regr_sxx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_sxx() { return &GenericExec<TPgDirectFunc<&float8_regr_sxx>, true, true, TArgs_float8_regr_sxx_Policy>; }
struct TArgs_float8_regr_sxy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_sxy() { return &GenericExec<TPgDirectFunc<&float8_regr_sxy>, true, true, TArgs_float8_regr_sxy_Policy>; }
struct TArgs_float8_regr_syy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_syy() { return &GenericExec<TPgDirectFunc<&float8_regr_syy>, true, true, TArgs_float8_regr_syy_Policy>; }
struct TArgs_float8_stddev_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_stddev_pop() { return &GenericExec<TPgDirectFunc<&float8_stddev_pop>, true, true, TArgs_float8_stddev_pop_Policy>; }
struct TArgs_float8_stddev_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_stddev_samp() { return &GenericExec<TPgDirectFunc<&float8_stddev_samp>, true, true, TArgs_float8_stddev_samp_Policy>; }
struct TArgs_float8_var_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_var_pop() { return &GenericExec<TPgDirectFunc<&float8_var_pop>, true, true, TArgs_float8_var_pop_Policy>; }
struct TArgs_float8_var_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_var_samp() { return &GenericExec<TPgDirectFunc<&float8_var_samp>, true, true, TArgs_float8_var_samp_Policy>; }
struct TArgs_float8abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8abs() { return &GenericExec<TPgDirectFunc<&float8abs>, true, true, TArgs_float8abs_Policy>; }
struct TArgs_float8div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8div() { return &GenericExec<TPgDirectFunc<&float8div>, true, true, TArgs_float8div_Policy>; }
struct TArgs_float8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8eq() { return &GenericExec<TPgDirectFunc<&float8eq>, true, true, TArgs_float8eq_Policy>; }
struct TArgs_float8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8ge() { return &GenericExec<TPgDirectFunc<&float8ge>, true, true, TArgs_float8ge_Policy>; }
struct TArgs_float8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8gt() { return &GenericExec<TPgDirectFunc<&float8gt>, true, true, TArgs_float8gt_Policy>; }
struct TArgs_float8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8in() { return &GenericExec<TPgDirectFunc<&float8in>, true, true, TArgs_float8in_Policy>; }
struct TArgs_float8larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8larger() { return &GenericExec<TPgDirectFunc<&float8larger>, true, true, TArgs_float8larger_Policy>; }
struct TArgs_float8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8le() { return &GenericExec<TPgDirectFunc<&float8le>, true, true, TArgs_float8le_Policy>; }
struct TArgs_float8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8lt() { return &GenericExec<TPgDirectFunc<&float8lt>, true, true, TArgs_float8lt_Policy>; }
struct TArgs_float8mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8mi() { return &GenericExec<TPgDirectFunc<&float8mi>, true, true, TArgs_float8mi_Policy>; }
struct TArgs_float8mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8mul() { return &GenericExec<TPgDirectFunc<&float8mul>, true, true, TArgs_float8mul_Policy>; }
struct TArgs_float8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8ne() { return &GenericExec<TPgDirectFunc<&float8ne>, true, true, TArgs_float8ne_Policy>; }
struct TArgs_float8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8out() { return &GenericExec<TPgDirectFunc<&float8out>, true, false, TArgs_float8out_Policy>; }
struct TArgs_float8pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8pl() { return &GenericExec<TPgDirectFunc<&float8pl>, true, true, TArgs_float8pl_Policy>; }
struct TArgs_float8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8recv() { return &GenericExec<TPgDirectFunc<&float8recv>, true, true, TArgs_float8recv_Policy>; }
struct TArgs_float8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8send() { return &GenericExec<TPgDirectFunc<&float8send>, true, false, TArgs_float8send_Policy>; }
struct TArgs_float8smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8smaller() { return &GenericExec<TPgDirectFunc<&float8smaller>, true, true, TArgs_float8smaller_Policy>; }
struct TArgs_float8um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8um() { return &GenericExec<TPgDirectFunc<&float8um>, true, true, TArgs_float8um_Policy>; }
struct TArgs_float8up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8up() { return &GenericExec<TPgDirectFunc<&float8up>, true, true, TArgs_float8up_Policy>; }
struct TArgs_flt4_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_flt4_mul_cash() { return &GenericExec<TPgDirectFunc<&flt4_mul_cash>, true, true, TArgs_flt4_mul_cash_Policy>; }
struct TArgs_flt8_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_flt8_mul_cash() { return &GenericExec<TPgDirectFunc<&flt8_mul_cash>, true, true, TArgs_flt8_mul_cash_Policy>; }
struct TArgs_format_type_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_format_type() { return &GenericExec<TPgDirectFunc<&format_type>, false, false, TArgs_format_type_Policy>; }
struct TArgs_ftod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftod() { return &GenericExec<TPgDirectFunc<&ftod>, true, true, TArgs_ftod_Policy>; }
struct TArgs_ftoi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi2() { return &GenericExec<TPgDirectFunc<&ftoi2>, true, true, TArgs_ftoi2_Policy>; }
struct TArgs_ftoi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi4() { return &GenericExec<TPgDirectFunc<&ftoi4>, true, true, TArgs_ftoi4_Policy>; }
struct TArgs_ginarrayconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_ginarrayconsistent() { return &GenericExec<TPgDirectFunc<&ginarrayconsistent>, true, true, TArgs_ginarrayconsistent_Policy>; }
struct TArgs_ginarrayextract_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_ginarrayextract() { return &GenericExec<TPgDirectFunc<&ginarrayextract>, true, true, TArgs_ginarrayextract_Policy>; }
struct TArgs_ginarrayextract_2args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_ginarrayextract_2args() { return &GenericExec<TPgDirectFunc<&ginarrayextract_2args>, true, true, TArgs_ginarrayextract_2args_Policy>; }
struct TArgs_ginarraytriconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_ginarraytriconsistent() { return &GenericExec<TPgDirectFunc<&ginarraytriconsistent>, true, true, TArgs_ginarraytriconsistent_Policy>; }
struct TArgs_ginhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ginhandler() { return &GenericExec<TPgDirectFunc<&ginhandler>, true, true, TArgs_ginhandler_Policy>; }
struct TArgs_ginqueryarrayextract_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_ginqueryarrayextract() { return &GenericExec<TPgDirectFunc<&ginqueryarrayextract>, true, true, TArgs_ginqueryarrayextract_Policy>; }
struct TArgs_hash_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_multirange() { return &GenericExec<TPgDirectFunc<&hash_multirange>, true, true, TArgs_hash_multirange_Policy>; }
struct TArgs_hash_multirange_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_multirange_extended() { return &GenericExec<TPgDirectFunc<&hash_multirange_extended>, true, true, TArgs_hash_multirange_extended_Policy>; }
struct TArgs_i2tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2tod() { return &GenericExec<TPgDirectFunc<&i2tod>, true, true, TArgs_i2tod_Policy>; }
struct TArgs_i2tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2tof() { return &GenericExec<TPgDirectFunc<&i2tof>, true, true, TArgs_i2tof_Policy>; }
struct TArgs_i4tochar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tochar() { return &GenericExec<TPgDirectFunc<&i4tochar>, true, true, TArgs_i4tochar_Policy>; }
struct TArgs_i4tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tod() { return &GenericExec<TPgDirectFunc<&i4tod>, true, true, TArgs_i4tod_Policy>; }
struct TArgs_i4tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tof() { return &GenericExec<TPgDirectFunc<&i4tof>, true, true, TArgs_i4tof_Policy>; }
struct TArgs_in_range_float4_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_float4_float8() { return &GenericExec<TPgDirectFunc<&in_range_float4_float8>, true, true, TArgs_in_range_float4_float8_Policy>; }
struct TArgs_in_range_float8_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_float8_float8() { return &GenericExec<TPgDirectFunc<&in_range_float8_float8>, true, true, TArgs_in_range_float8_float8_Policy>; }
struct TArgs_int2_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2_mul_cash() { return &GenericExec<TPgDirectFunc<&int2_mul_cash>, true, true, TArgs_int2_mul_cash_Policy>; }
struct TArgs_int4_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4_cash() { return &GenericExec<TPgDirectFunc<&int4_cash>, true, true, TArgs_int4_cash_Policy>; }
struct TArgs_int4_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4_mul_cash() { return &GenericExec<TPgDirectFunc<&int4_mul_cash>, true, true, TArgs_int4_mul_cash_Policy>; }
struct TArgs_int8_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8_cash() { return &GenericExec<TPgDirectFunc<&int8_cash>, true, true, TArgs_int8_cash_Policy>; }
struct TArgs_int8_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_mul_cash() { return &GenericExec<TPgDirectFunc<&int8_mul_cash>, true, true, TArgs_int8_mul_cash_Policy>; }
struct TArgs_json_array_element_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_json_array_element() { return &GenericExec<TPgDirectFunc<&json_array_element>, true, false, TArgs_json_array_element_Policy>; }
struct TArgs_json_array_element_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_json_array_element_text() { return &GenericExec<TPgDirectFunc<&json_array_element_text>, true, false, TArgs_json_array_element_text_Policy>; }
struct TArgs_json_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_array_length() { return &GenericExec<TPgDirectFunc<&json_array_length>, true, true, TArgs_json_array_length_Policy>; }
struct TArgs_json_object_field_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_json_object_field() { return &GenericExec<TPgDirectFunc<&json_object_field>, true, false, TArgs_json_object_field_Policy>; }
struct TArgs_json_object_field_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_json_object_field_text() { return &GenericExec<TPgDirectFunc<&json_object_field_text>, true, false, TArgs_json_object_field_text_Policy>; }
struct TArgs_json_populate_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_json_populate_record() { return &GenericExec<TPgDirectFunc<&json_populate_record>, false, true, TArgs_json_populate_record_Policy>; }
struct TArgs_json_strip_nulls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_strip_nulls() { return &GenericExec<TPgDirectFunc<&json_strip_nulls>, true, false, TArgs_json_strip_nulls_Policy>; }
struct TArgs_json_to_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_to_record() { return &GenericExec<TPgDirectFunc<&json_to_record>, true, false, TArgs_json_to_record_Policy>; }
struct TArgs_jsonb_array_element_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_jsonb_array_element() { return &GenericExec<TPgDirectFunc<&jsonb_array_element>, true, false, TArgs_jsonb_array_element_Policy>; }
struct TArgs_jsonb_array_element_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_jsonb_array_element_text() { return &GenericExec<TPgDirectFunc<&jsonb_array_element_text>, true, false, TArgs_jsonb_array_element_text_Policy>; }
struct TArgs_jsonb_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_array_length() { return &GenericExec<TPgDirectFunc<&jsonb_array_length>, true, true, TArgs_jsonb_array_length_Policy>; }
struct TArgs_jsonb_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_cmp() { return &GenericExec<TPgDirectFunc<&jsonb_cmp>, true, true, TArgs_jsonb_cmp_Policy>; }
struct TArgs_jsonb_concat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_concat() { return &GenericExec<TPgDirectFunc<&jsonb_concat>, true, false, TArgs_jsonb_concat_Policy>; }
struct TArgs_jsonb_contained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_contained() { return &GenericExec<TPgDirectFunc<&jsonb_contained>, true, true, TArgs_jsonb_contained_Policy>; }
struct TArgs_jsonb_contains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_contains() { return &GenericExec<TPgDirectFunc<&jsonb_contains>, true, true, TArgs_jsonb_contains_Policy>; }
struct TArgs_jsonb_delete_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_delete() { return &GenericExec<TPgDirectFunc<&jsonb_delete>, true, false, TArgs_jsonb_delete_Policy>; }
struct TArgs_jsonb_delete_idx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_jsonb_delete_idx() { return &GenericExec<TPgDirectFunc<&jsonb_delete_idx>, true, false, TArgs_jsonb_delete_idx_Policy>; }
struct TArgs_jsonb_delete_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_delete_path() { return &GenericExec<TPgDirectFunc<&jsonb_delete_path>, true, false, TArgs_jsonb_delete_path_Policy>; }
struct TArgs_jsonb_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_eq() { return &GenericExec<TPgDirectFunc<&jsonb_eq>, true, true, TArgs_jsonb_eq_Policy>; }
struct TArgs_jsonb_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists() { return &GenericExec<TPgDirectFunc<&jsonb_exists>, true, true, TArgs_jsonb_exists_Policy>; }
struct TArgs_jsonb_exists_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists_all() { return &GenericExec<TPgDirectFunc<&jsonb_exists_all>, true, true, TArgs_jsonb_exists_all_Policy>; }
struct TArgs_jsonb_exists_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists_any() { return &GenericExec<TPgDirectFunc<&jsonb_exists_any>, true, true, TArgs_jsonb_exists_any_Policy>; }
struct TArgs_jsonb_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_ge() { return &GenericExec<TPgDirectFunc<&jsonb_ge>, true, true, TArgs_jsonb_ge_Policy>; }
struct TArgs_jsonb_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_gt() { return &GenericExec<TPgDirectFunc<&jsonb_gt>, true, true, TArgs_jsonb_gt_Policy>; }
struct TArgs_jsonb_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_hash() { return &GenericExec<TPgDirectFunc<&jsonb_hash>, true, true, TArgs_jsonb_hash_Policy>; }
struct TArgs_jsonb_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_jsonb_hash_extended() { return &GenericExec<TPgDirectFunc<&jsonb_hash_extended>, true, true, TArgs_jsonb_hash_extended_Policy>; }
struct TArgs_jsonb_insert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_insert() { return &GenericExec<TPgDirectFunc<&jsonb_insert>, true, false, TArgs_jsonb_insert_Policy>; }
struct TArgs_jsonb_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_le() { return &GenericExec<TPgDirectFunc<&jsonb_le>, true, true, TArgs_jsonb_le_Policy>; }
struct TArgs_jsonb_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_lt() { return &GenericExec<TPgDirectFunc<&jsonb_lt>, true, true, TArgs_jsonb_lt_Policy>; }
struct TArgs_jsonb_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_ne() { return &GenericExec<TPgDirectFunc<&jsonb_ne>, true, true, TArgs_jsonb_ne_Policy>; }
struct TArgs_jsonb_object_field_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_object_field() { return &GenericExec<TPgDirectFunc<&jsonb_object_field>, true, false, TArgs_jsonb_object_field_Policy>; }
struct TArgs_jsonb_object_field_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_object_field_text() { return &GenericExec<TPgDirectFunc<&jsonb_object_field_text>, true, false, TArgs_jsonb_object_field_text_Policy>; }
struct TArgs_jsonb_populate_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_jsonb_populate_record() { return &GenericExec<TPgDirectFunc<&jsonb_populate_record>, false, true, TArgs_jsonb_populate_record_Policy>; }
struct TArgs_jsonb_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_pretty() { return &GenericExec<TPgDirectFunc<&jsonb_pretty>, true, false, TArgs_jsonb_pretty_Policy>; }
struct TArgs_jsonb_set_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_set() { return &GenericExec<TPgDirectFunc<&jsonb_set>, true, false, TArgs_jsonb_set_Policy>; }
struct TArgs_jsonb_set_lax_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,false};
};
TExecFunc arrow_jsonb_set_lax() { return &GenericExec<TPgDirectFunc<&jsonb_set_lax>, false, false, TArgs_jsonb_set_lax_Policy>; }
struct TArgs_jsonb_strip_nulls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_strip_nulls() { return &GenericExec<TPgDirectFunc<&jsonb_strip_nulls>, true, false, TArgs_jsonb_strip_nulls_Policy>; }
struct TArgs_jsonb_to_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_to_record() { return &GenericExec<TPgDirectFunc<&jsonb_to_record>, true, false, TArgs_jsonb_to_record_Policy>; }
struct TArgs_jsonpath_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonpath_in() { return &GenericExec<TPgDirectFunc<&jsonpath_in>, true, false, TArgs_jsonpath_in_Policy>; }
struct TArgs_jsonpath_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonpath_out() { return &GenericExec<TPgDirectFunc<&jsonpath_out>, true, false, TArgs_jsonpath_out_Policy>; }
struct TArgs_jsonpath_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonpath_recv() { return &GenericExec<TPgDirectFunc<&jsonpath_recv>, true, false, TArgs_jsonpath_recv_Policy>; }
struct TArgs_jsonpath_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonpath_send() { return &GenericExec<TPgDirectFunc<&jsonpath_send>, true, false, TArgs_jsonpath_send_Policy>; }
struct TArgs_multirange_adjacent_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_adjacent_multirange() { return &GenericExec<TPgDirectFunc<&multirange_adjacent_multirange>, true, true, TArgs_multirange_adjacent_multirange_Policy>; }
struct TArgs_multirange_adjacent_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_adjacent_range() { return &GenericExec<TPgDirectFunc<&multirange_adjacent_range>, true, true, TArgs_multirange_adjacent_range_Policy>; }
struct TArgs_multirange_after_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_after_multirange() { return &GenericExec<TPgDirectFunc<&multirange_after_multirange>, true, true, TArgs_multirange_after_multirange_Policy>; }
struct TArgs_multirange_after_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_after_range() { return &GenericExec<TPgDirectFunc<&multirange_after_range>, true, true, TArgs_multirange_after_range_Policy>; }
struct TArgs_multirange_before_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_before_multirange() { return &GenericExec<TPgDirectFunc<&multirange_before_multirange>, true, true, TArgs_multirange_before_multirange_Policy>; }
struct TArgs_multirange_before_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_before_range() { return &GenericExec<TPgDirectFunc<&multirange_before_range>, true, true, TArgs_multirange_before_range_Policy>; }
struct TArgs_multirange_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_cmp() { return &GenericExec<TPgDirectFunc<&multirange_cmp>, true, true, TArgs_multirange_cmp_Policy>; }
struct TArgs_multirange_constructor0_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_multirange_constructor0() { return &GenericExec<TPgDirectFunc<&multirange_constructor0>, true, false, TArgs_multirange_constructor0_Policy>; }
struct TArgs_multirange_constructor1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_constructor1() { return &GenericExec<TPgDirectFunc<&multirange_constructor1>, true, false, TArgs_multirange_constructor1_Policy>; }
struct TArgs_multirange_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contained_by_multirange() { return &GenericExec<TPgDirectFunc<&multirange_contained_by_multirange>, true, true, TArgs_multirange_contained_by_multirange_Policy>; }
struct TArgs_multirange_contained_by_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contained_by_range() { return &GenericExec<TPgDirectFunc<&multirange_contained_by_range>, true, true, TArgs_multirange_contained_by_range_Policy>; }
struct TArgs_multirange_contains_elem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_multirange_contains_elem() { return &GenericExec<TPgDirectFunc<&multirange_contains_elem>, true, true, TArgs_multirange_contains_elem_Policy>; }
struct TArgs_multirange_contains_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contains_multirange() { return &GenericExec<TPgDirectFunc<&multirange_contains_multirange>, true, true, TArgs_multirange_contains_multirange_Policy>; }
struct TArgs_multirange_contains_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contains_range() { return &GenericExec<TPgDirectFunc<&multirange_contains_range>, true, true, TArgs_multirange_contains_range_Policy>; }
struct TArgs_multirange_empty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_empty() { return &GenericExec<TPgDirectFunc<&multirange_empty>, true, true, TArgs_multirange_empty_Policy>; }
struct TArgs_multirange_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_eq() { return &GenericExec<TPgDirectFunc<&multirange_eq>, true, true, TArgs_multirange_eq_Policy>; }
struct TArgs_multirange_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_ge() { return &GenericExec<TPgDirectFunc<&multirange_ge>, true, true, TArgs_multirange_ge_Policy>; }
struct TArgs_multirange_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_gt() { return &GenericExec<TPgDirectFunc<&multirange_gt>, true, true, TArgs_multirange_gt_Policy>; }
struct TArgs_multirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_multirange_in() { return &GenericExec<TPgDirectFunc<&multirange_in>, true, false, TArgs_multirange_in_Policy>; }
struct TArgs_multirange_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_intersect() { return &GenericExec<TPgDirectFunc<&multirange_intersect>, true, false, TArgs_multirange_intersect_Policy>; }
struct TArgs_multirange_intersect_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_intersect_agg_transfn() { return &GenericExec<TPgDirectFunc<&multirange_intersect_agg_transfn>, true, false, TArgs_multirange_intersect_agg_transfn_Policy>; }
struct TArgs_multirange_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_le() { return &GenericExec<TPgDirectFunc<&multirange_le>, true, true, TArgs_multirange_le_Policy>; }
struct TArgs_multirange_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower() { return &GenericExec<TPgDirectFunc<&multirange_lower>, true, true, TArgs_multirange_lower_Policy>; }
struct TArgs_multirange_lower_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower_inc() { return &GenericExec<TPgDirectFunc<&multirange_lower_inc>, true, true, TArgs_multirange_lower_inc_Policy>; }
struct TArgs_multirange_lower_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower_inf() { return &GenericExec<TPgDirectFunc<&multirange_lower_inf>, true, true, TArgs_multirange_lower_inf_Policy>; }
struct TArgs_multirange_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_lt() { return &GenericExec<TPgDirectFunc<&multirange_lt>, true, true, TArgs_multirange_lt_Policy>; }
struct TArgs_multirange_minus_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_minus() { return &GenericExec<TPgDirectFunc<&multirange_minus>, true, false, TArgs_multirange_minus_Policy>; }
struct TArgs_multirange_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_ne() { return &GenericExec<TPgDirectFunc<&multirange_ne>, true, true, TArgs_multirange_ne_Policy>; }
struct TArgs_multirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_out() { return &GenericExec<TPgDirectFunc<&multirange_out>, true, false, TArgs_multirange_out_Policy>; }
struct TArgs_multirange_overlaps_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overlaps_multirange() { return &GenericExec<TPgDirectFunc<&multirange_overlaps_multirange>, true, true, TArgs_multirange_overlaps_multirange_Policy>; }
struct TArgs_multirange_overlaps_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overlaps_range() { return &GenericExec<TPgDirectFunc<&multirange_overlaps_range>, true, true, TArgs_multirange_overlaps_range_Policy>; }
struct TArgs_multirange_overleft_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overleft_multirange() { return &GenericExec<TPgDirectFunc<&multirange_overleft_multirange>, true, true, TArgs_multirange_overleft_multirange_Policy>; }
struct TArgs_multirange_overleft_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overleft_range() { return &GenericExec<TPgDirectFunc<&multirange_overleft_range>, true, true, TArgs_multirange_overleft_range_Policy>; }
struct TArgs_multirange_overright_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overright_multirange() { return &GenericExec<TPgDirectFunc<&multirange_overright_multirange>, true, true, TArgs_multirange_overright_multirange_Policy>; }
struct TArgs_multirange_overright_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overright_range() { return &GenericExec<TPgDirectFunc<&multirange_overright_range>, true, true, TArgs_multirange_overright_range_Policy>; }
struct TArgs_multirange_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_multirange_recv() { return &GenericExec<TPgDirectFunc<&multirange_recv>, true, false, TArgs_multirange_recv_Policy>; }
struct TArgs_multirange_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_send() { return &GenericExec<TPgDirectFunc<&multirange_send>, true, false, TArgs_multirange_send_Policy>; }
struct TArgs_multirange_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_union() { return &GenericExec<TPgDirectFunc<&multirange_union>, true, false, TArgs_multirange_union_Policy>; }
struct TArgs_multirange_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper() { return &GenericExec<TPgDirectFunc<&multirange_upper>, true, true, TArgs_multirange_upper_Policy>; }
struct TArgs_multirange_upper_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper_inc() { return &GenericExec<TPgDirectFunc<&multirange_upper_inc>, true, true, TArgs_multirange_upper_inc_Policy>; }
struct TArgs_multirange_upper_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper_inf() { return &GenericExec<TPgDirectFunc<&multirange_upper_inf>, true, true, TArgs_multirange_upper_inf_Policy>; }
struct TArgs_name_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_name_text() { return &GenericExec<TPgDirectFunc<&name_text>, true, false, TArgs_name_text_Policy>; }
struct TArgs_nameeqtext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameeqtext() { return &GenericExec<TPgDirectFunc<&nameeqtext>, true, true, TArgs_nameeqtext_Policy>; }
struct TArgs_namegetext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegetext() { return &GenericExec<TPgDirectFunc<&namegetext>, true, true, TArgs_namegetext_Policy>; }
struct TArgs_namegttext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegttext() { return &GenericExec<TPgDirectFunc<&namegttext>, true, true, TArgs_namegttext_Policy>; }
struct TArgs_nameicregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameicregexeq() { return &GenericExec<TPgDirectFunc<&nameicregexeq>, true, true, TArgs_nameicregexeq_Policy>; }
struct TArgs_nameicregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameicregexne() { return &GenericExec<TPgDirectFunc<&nameicregexne>, true, true, TArgs_nameicregexne_Policy>; }
struct TArgs_nameletext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameletext() { return &GenericExec<TPgDirectFunc<&nameletext>, true, true, TArgs_nameletext_Policy>; }
struct TArgs_namelttext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelttext() { return &GenericExec<TPgDirectFunc<&namelttext>, true, true, TArgs_namelttext_Policy>; }
struct TArgs_namenetext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namenetext() { return &GenericExec<TPgDirectFunc<&namenetext>, true, true, TArgs_namenetext_Policy>; }
struct TArgs_nameregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameregexeq() { return &GenericExec<TPgDirectFunc<&nameregexeq>, true, true, TArgs_nameregexeq_Policy>; }
struct TArgs_nameregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameregexne() { return &GenericExec<TPgDirectFunc<&nameregexne>, true, true, TArgs_nameregexne_Policy>; }
struct TArgs_numeric_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_cash() { return &GenericExec<TPgDirectFunc<&numeric_cash>, true, true, TArgs_numeric_cash_Policy>; }
struct TArgs_oideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oideq() { return &GenericExec<TPgDirectFunc<&oideq>, true, true, TArgs_oideq_Policy>; }
struct TArgs_oidge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidge() { return &GenericExec<TPgDirectFunc<&oidge>, true, true, TArgs_oidge_Policy>; }
struct TArgs_oidgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidgt() { return &GenericExec<TPgDirectFunc<&oidgt>, true, true, TArgs_oidgt_Policy>; }
struct TArgs_oidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_oidin() { return &GenericExec<TPgDirectFunc<&oidin>, true, true, TArgs_oidin_Policy>; }
struct TArgs_oidlarger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidlarger() { return &GenericExec<TPgDirectFunc<&oidlarger>, true, true, TArgs_oidlarger_Policy>; }
struct TArgs_oidle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidle() { return &GenericExec<TPgDirectFunc<&oidle>, true, true, TArgs_oidle_Policy>; }
struct TArgs_oidlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidlt() { return &GenericExec<TPgDirectFunc<&oidlt>, true, true, TArgs_oidlt_Policy>; }
struct TArgs_oidne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidne() { return &GenericExec<TPgDirectFunc<&oidne>, true, true, TArgs_oidne_Policy>; }
struct TArgs_oidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidout() { return &GenericExec<TPgDirectFunc<&oidout>, true, false, TArgs_oidout_Policy>; }
struct TArgs_oidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidrecv() { return &GenericExec<TPgDirectFunc<&oidrecv>, true, true, TArgs_oidrecv_Policy>; }
struct TArgs_oidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidsend() { return &GenericExec<TPgDirectFunc<&oidsend>, true, false, TArgs_oidsend_Policy>; }
struct TArgs_oidsmaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_oidsmaller() { return &GenericExec<TPgDirectFunc<&oidsmaller>, true, true, TArgs_oidsmaller_Policy>; }
struct TArgs_oidvectoreq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectoreq() { return &GenericExec<TPgDirectFunc<&oidvectoreq>, true, true, TArgs_oidvectoreq_Policy>; }
struct TArgs_oidvectorge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectorge() { return &GenericExec<TPgDirectFunc<&oidvectorge>, true, true, TArgs_oidvectorge_Policy>; }
struct TArgs_oidvectorgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectorgt() { return &GenericExec<TPgDirectFunc<&oidvectorgt>, true, true, TArgs_oidvectorgt_Policy>; }
struct TArgs_oidvectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_oidvectorin() { return &GenericExec<TPgDirectFunc<&oidvectorin>, true, false, TArgs_oidvectorin_Policy>; }
struct TArgs_oidvectorle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectorle() { return &GenericExec<TPgDirectFunc<&oidvectorle>, true, true, TArgs_oidvectorle_Policy>; }
struct TArgs_oidvectorlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectorlt() { return &GenericExec<TPgDirectFunc<&oidvectorlt>, true, true, TArgs_oidvectorlt_Policy>; }
struct TArgs_oidvectorne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_oidvectorne() { return &GenericExec<TPgDirectFunc<&oidvectorne>, true, true, TArgs_oidvectorne_Policy>; }
struct TArgs_oidvectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_oidvectorout() { return &GenericExec<TPgDirectFunc<&oidvectorout>, true, false, TArgs_oidvectorout_Policy>; }
struct TArgs_oidvectorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidvectorrecv() { return &GenericExec<TPgDirectFunc<&oidvectorrecv>, true, false, TArgs_oidvectorrecv_Policy>; }
struct TArgs_oidvectorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_oidvectorsend() { return &GenericExec<TPgDirectFunc<&oidvectorsend>, true, false, TArgs_oidvectorsend_Policy>; }
struct TArgs_oidvectortypes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_oidvectortypes() { return &GenericExec<TPgDirectFunc<&oidvectortypes>, true, false, TArgs_oidvectortypes_Policy>; }
struct TArgs_pg_advisory_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_lock_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_lock_int4>, true, true, TArgs_pg_advisory_lock_int4_Policy>; }
struct TArgs_pg_advisory_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_lock_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_lock_int8>, true, true, TArgs_pg_advisory_lock_int8_Policy>; }
struct TArgs_pg_advisory_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_lock_shared_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_lock_shared_int4>, true, true, TArgs_pg_advisory_lock_shared_int4_Policy>; }
struct TArgs_pg_advisory_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_lock_shared_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_lock_shared_int8>, true, true, TArgs_pg_advisory_lock_shared_int8_Policy>; }
struct TArgs_pg_advisory_unlock_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_advisory_unlock_all() { return &GenericExec<TPgDirectFunc<&pg_advisory_unlock_all>, true, true, TArgs_pg_advisory_unlock_all_Policy>; }
struct TArgs_pg_advisory_unlock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_unlock_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_unlock_int4>, true, true, TArgs_pg_advisory_unlock_int4_Policy>; }
struct TArgs_pg_advisory_unlock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_unlock_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_unlock_int8>, true, true, TArgs_pg_advisory_unlock_int8_Policy>; }
struct TArgs_pg_advisory_unlock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_unlock_shared_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_unlock_shared_int4>, true, true, TArgs_pg_advisory_unlock_shared_int4_Policy>; }
struct TArgs_pg_advisory_unlock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_unlock_shared_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_unlock_shared_int8>, true, true, TArgs_pg_advisory_unlock_shared_int8_Policy>; }
struct TArgs_pg_advisory_xact_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_xact_lock_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_xact_lock_int4>, true, true, TArgs_pg_advisory_xact_lock_int4_Policy>; }
struct TArgs_pg_advisory_xact_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_xact_lock_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_xact_lock_int8>, true, true, TArgs_pg_advisory_xact_lock_int8_Policy>; }
struct TArgs_pg_advisory_xact_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_xact_lock_shared_int4() { return &GenericExec<TPgDirectFunc<&pg_advisory_xact_lock_shared_int4>, true, true, TArgs_pg_advisory_xact_lock_shared_int4_Policy>; }
struct TArgs_pg_advisory_xact_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_xact_lock_shared_int8() { return &GenericExec<TPgDirectFunc<&pg_advisory_xact_lock_shared_int8>, true, true, TArgs_pg_advisory_xact_lock_shared_int8_Policy>; }
struct TArgs_pg_backup_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backup_start_time() { return &GenericExec<TPgDirectFunc<&pg_backup_start_time>, true, true, TArgs_pg_backup_start_time_Policy>; }
struct TArgs_pg_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_blocking_pids() { return &GenericExec<TPgDirectFunc<&pg_blocking_pids>, true, false, TArgs_pg_blocking_pids_Policy>; }
struct TArgs_pg_collation_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_is_visible() { return &GenericExec<TPgDirectFunc<&pg_collation_is_visible>, true, true, TArgs_pg_collation_is_visible_Policy>; }
struct TArgs_pg_column_compression_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_column_compression() { return &GenericExec<TPgDirectFunc<&pg_column_compression>, true, false, TArgs_pg_column_compression_Policy>; }
struct TArgs_pg_column_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_column_size() { return &GenericExec<TPgDirectFunc<&pg_column_size>, true, true, TArgs_pg_column_size_Policy>; }
struct TArgs_pg_control_checkpoint_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_checkpoint() { return &GenericExec<TPgDirectFunc<&pg_control_checkpoint>, true, false, TArgs_pg_control_checkpoint_Policy>; }
struct TArgs_pg_control_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_init() { return &GenericExec<TPgDirectFunc<&pg_control_init>, true, false, TArgs_pg_control_init_Policy>; }
struct TArgs_pg_control_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_recovery() { return &GenericExec<TPgDirectFunc<&pg_control_recovery>, true, false, TArgs_pg_control_recovery_Policy>; }
struct TArgs_pg_control_system_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_system() { return &GenericExec<TPgDirectFunc<&pg_control_system>, true, false, TArgs_pg_control_system_Policy>; }
struct TArgs_pg_conversion_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_conversion_is_visible() { return &GenericExec<TPgDirectFunc<&pg_conversion_is_visible>, true, true, TArgs_pg_conversion_is_visible_Policy>; }
struct TArgs_pg_create_restore_point_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_create_restore_point() { return &GenericExec<TPgDirectFunc<&pg_create_restore_point>, true, true, TArgs_pg_create_restore_point_Policy>; }
struct TArgs_pg_current_wal_flush_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_flush_lsn() { return &GenericExec<TPgDirectFunc<&pg_current_wal_flush_lsn>, true, true, TArgs_pg_current_wal_flush_lsn_Policy>; }
struct TArgs_pg_current_wal_insert_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_insert_lsn() { return &GenericExec<TPgDirectFunc<&pg_current_wal_insert_lsn>, true, true, TArgs_pg_current_wal_insert_lsn_Policy>; }
struct TArgs_pg_current_wal_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_lsn() { return &GenericExec<TPgDirectFunc<&pg_current_wal_lsn>, true, true, TArgs_pg_current_wal_lsn_Policy>; }
struct TArgs_pg_function_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_function_is_visible() { return &GenericExec<TPgDirectFunc<&pg_function_is_visible>, true, true, TArgs_pg_function_is_visible_Policy>; }
struct TArgs_pg_get_wal_replay_pause_state_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_get_wal_replay_pause_state() { return &GenericExec<TPgDirectFunc<&pg_get_wal_replay_pause_state>, true, false, TArgs_pg_get_wal_replay_pause_state_Policy>; }
struct TArgs_pg_index_column_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_pg_index_column_has_property() { return &GenericExec<TPgDirectFunc<&pg_index_column_has_property>, true, true, TArgs_pg_index_column_has_property_Policy>; }
struct TArgs_pg_index_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_index_has_property() { return &GenericExec<TPgDirectFunc<&pg_index_has_property>, true, true, TArgs_pg_index_has_property_Policy>; }
struct TArgs_pg_indexam_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_indexam_has_property() { return &GenericExec<TPgDirectFunc<&pg_indexam_has_property>, true, true, TArgs_pg_indexam_has_property_Policy>; }
struct TArgs_pg_indexam_progress_phasename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_indexam_progress_phasename() { return &GenericExec<TPgDirectFunc<&pg_indexam_progress_phasename>, true, false, TArgs_pg_indexam_progress_phasename_Policy>; }
struct TArgs_pg_is_in_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_in_backup() { return &GenericExec<TPgDirectFunc<&pg_is_in_backup>, true, true, TArgs_pg_is_in_backup_Policy>; }
struct TArgs_pg_is_in_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_in_recovery() { return &GenericExec<TPgDirectFunc<&pg_is_in_recovery>, true, true, TArgs_pg_is_in_recovery_Policy>; }
struct TArgs_pg_is_other_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_is_other_temp_schema() { return &GenericExec<TPgDirectFunc<&pg_is_other_temp_schema>, true, true, TArgs_pg_is_other_temp_schema_Policy>; }
struct TArgs_pg_is_wal_replay_paused_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_wal_replay_paused() { return &GenericExec<TPgDirectFunc<&pg_is_wal_replay_paused>, true, true, TArgs_pg_is_wal_replay_paused_Policy>; }
struct TArgs_pg_isolation_test_session_is_blocked_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_isolation_test_session_is_blocked() { return &GenericExec<TPgDirectFunc<&pg_isolation_test_session_is_blocked>, true, true, TArgs_pg_isolation_test_session_is_blocked_Policy>; }
struct TArgs_pg_jit_available_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_jit_available() { return &GenericExec<TPgDirectFunc<&pg_jit_available>, true, true, TArgs_pg_jit_available_Policy>; }
struct TArgs_pg_last_committed_xact_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_committed_xact() { return &GenericExec<TPgDirectFunc<&pg_last_committed_xact>, true, false, TArgs_pg_last_committed_xact_Policy>; }
struct TArgs_pg_last_wal_receive_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_wal_receive_lsn() { return &GenericExec<TPgDirectFunc<&pg_last_wal_receive_lsn>, true, true, TArgs_pg_last_wal_receive_lsn_Policy>; }
struct TArgs_pg_last_wal_replay_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_wal_replay_lsn() { return &GenericExec<TPgDirectFunc<&pg_last_wal_replay_lsn>, true, true, TArgs_pg_last_wal_replay_lsn_Policy>; }
struct TArgs_pg_last_xact_replay_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_xact_replay_timestamp() { return &GenericExec<TPgDirectFunc<&pg_last_xact_replay_timestamp>, true, true, TArgs_pg_last_xact_replay_timestamp_Policy>; }
struct TArgs_pg_lsn_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_cmp() { return &GenericExec<TPgDirectFunc<&pg_lsn_cmp>, true, true, TArgs_pg_lsn_cmp_Policy>; }
struct TArgs_pg_lsn_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_eq() { return &GenericExec<TPgDirectFunc<&pg_lsn_eq>, true, true, TArgs_pg_lsn_eq_Policy>; }
struct TArgs_pg_lsn_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_ge() { return &GenericExec<TPgDirectFunc<&pg_lsn_ge>, true, true, TArgs_pg_lsn_ge_Policy>; }
struct TArgs_pg_lsn_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_gt() { return &GenericExec<TPgDirectFunc<&pg_lsn_gt>, true, true, TArgs_pg_lsn_gt_Policy>; }
struct TArgs_pg_lsn_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_hash() { return &GenericExec<TPgDirectFunc<&pg_lsn_hash>, true, true, TArgs_pg_lsn_hash_Policy>; }
struct TArgs_pg_lsn_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_hash_extended() { return &GenericExec<TPgDirectFunc<&pg_lsn_hash_extended>, true, true, TArgs_pg_lsn_hash_extended_Policy>; }
struct TArgs_pg_lsn_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_lsn_in() { return &GenericExec<TPgDirectFunc<&pg_lsn_in>, true, true, TArgs_pg_lsn_in_Policy>; }
struct TArgs_pg_lsn_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_larger() { return &GenericExec<TPgDirectFunc<&pg_lsn_larger>, true, true, TArgs_pg_lsn_larger_Policy>; }
struct TArgs_pg_lsn_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_le() { return &GenericExec<TPgDirectFunc<&pg_lsn_le>, true, true, TArgs_pg_lsn_le_Policy>; }
struct TArgs_pg_lsn_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_lt() { return &GenericExec<TPgDirectFunc<&pg_lsn_lt>, true, true, TArgs_pg_lsn_lt_Policy>; }
struct TArgs_pg_lsn_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_mi() { return &GenericExec<TPgDirectFunc<&pg_lsn_mi>, true, false, TArgs_pg_lsn_mi_Policy>; }
struct TArgs_pg_lsn_mii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_lsn_mii() { return &GenericExec<TPgDirectFunc<&pg_lsn_mii>, true, true, TArgs_pg_lsn_mii_Policy>; }
struct TArgs_pg_lsn_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_ne() { return &GenericExec<TPgDirectFunc<&pg_lsn_ne>, true, true, TArgs_pg_lsn_ne_Policy>; }
struct TArgs_pg_lsn_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_out() { return &GenericExec<TPgDirectFunc<&pg_lsn_out>, true, false, TArgs_pg_lsn_out_Policy>; }
struct TArgs_pg_lsn_pli_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_lsn_pli() { return &GenericExec<TPgDirectFunc<&pg_lsn_pli>, true, true, TArgs_pg_lsn_pli_Policy>; }
struct TArgs_pg_lsn_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_recv() { return &GenericExec<TPgDirectFunc<&pg_lsn_recv>, true, true, TArgs_pg_lsn_recv_Policy>; }
struct TArgs_pg_lsn_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_send() { return &GenericExec<TPgDirectFunc<&pg_lsn_send>, true, false, TArgs_pg_lsn_send_Policy>; }
struct TArgs_pg_lsn_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_smaller() { return &GenericExec<TPgDirectFunc<&pg_lsn_smaller>, true, true, TArgs_pg_lsn_smaller_Policy>; }
struct TArgs_pg_my_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_my_temp_schema() { return &GenericExec<TPgDirectFunc<&pg_my_temp_schema>, true, true, TArgs_pg_my_temp_schema_Policy>; }
struct TArgs_pg_notification_queue_usage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_notification_queue_usage() { return &GenericExec<TPgDirectFunc<&pg_notification_queue_usage>, true, true, TArgs_pg_notification_queue_usage_Policy>; }
struct TArgs_pg_notify_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_notify() { return &GenericExec<TPgDirectFunc<&pg_notify>, false, true, TArgs_pg_notify_Policy>; }
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
struct TArgs_pg_partition_root_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_partition_root() { return &GenericExec<TPgDirectFunc<&pg_partition_root>, true, true, TArgs_pg_partition_root_Policy>; }
struct TArgs_pg_promote_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_promote() { return &GenericExec<TPgDirectFunc<&pg_promote>, true, true, TArgs_pg_promote_Policy>; }
struct TArgs_pg_relation_is_publishable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_is_publishable() { return &GenericExec<TPgDirectFunc<&pg_relation_is_publishable>, true, true, TArgs_pg_relation_is_publishable_Policy>; }
struct TArgs_pg_safe_snapshot_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_safe_snapshot_blocking_pids() { return &GenericExec<TPgDirectFunc<&pg_safe_snapshot_blocking_pids>, true, false, TArgs_pg_safe_snapshot_blocking_pids_Policy>; }
struct TArgs_pg_start_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_start_backup() { return &GenericExec<TPgDirectFunc<&pg_start_backup>, true, true, TArgs_pg_start_backup_Policy>; }
struct TArgs_pg_statistics_obj_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_statistics_obj_is_visible() { return &GenericExec<TPgDirectFunc<&pg_statistics_obj_is_visible>, true, true, TArgs_pg_statistics_obj_is_visible_Policy>; }
struct TArgs_pg_stop_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stop_backup() { return &GenericExec<TPgDirectFunc<&pg_stop_backup>, true, true, TArgs_pg_stop_backup_Policy>; }
struct TArgs_pg_switch_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_switch_wal() { return &GenericExec<TPgDirectFunc<&pg_switch_wal>, true, true, TArgs_pg_switch_wal_Policy>; }
struct TArgs_pg_table_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_is_visible() { return &GenericExec<TPgDirectFunc<&pg_table_is_visible>, true, true, TArgs_pg_table_is_visible_Policy>; }
struct TArgs_pg_try_advisory_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_lock_int4() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_lock_int4>, true, true, TArgs_pg_try_advisory_lock_int4_Policy>; }
struct TArgs_pg_try_advisory_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_lock_int8() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_lock_int8>, true, true, TArgs_pg_try_advisory_lock_int8_Policy>; }
struct TArgs_pg_try_advisory_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_lock_shared_int4() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_lock_shared_int4>, true, true, TArgs_pg_try_advisory_lock_shared_int4_Policy>; }
struct TArgs_pg_try_advisory_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_lock_shared_int8() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_lock_shared_int8>, true, true, TArgs_pg_try_advisory_lock_shared_int8_Policy>; }
struct TArgs_pg_try_advisory_xact_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_int4() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_int4>, true, true, TArgs_pg_try_advisory_xact_lock_int4_Policy>; }
struct TArgs_pg_try_advisory_xact_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_int8() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_int8>, true, true, TArgs_pg_try_advisory_xact_lock_int8_Policy>; }
struct TArgs_pg_try_advisory_xact_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_shared_int4() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_shared_int4>, true, true, TArgs_pg_try_advisory_xact_lock_shared_int4_Policy>; }
struct TArgs_pg_try_advisory_xact_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_shared_int8() { return &GenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_shared_int8>, true, true, TArgs_pg_try_advisory_xact_lock_shared_int8_Policy>; }
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
struct TArgs_pg_wal_lsn_diff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_wal_lsn_diff() { return &GenericExec<TPgDirectFunc<&pg_wal_lsn_diff>, true, false, TArgs_pg_wal_lsn_diff_Policy>; }
struct TArgs_pg_wal_replay_pause_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_wal_replay_pause() { return &GenericExec<TPgDirectFunc<&pg_wal_replay_pause>, true, true, TArgs_pg_wal_replay_pause_Policy>; }
struct TArgs_pg_wal_replay_resume_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_wal_replay_resume() { return &GenericExec<TPgDirectFunc<&pg_wal_replay_resume>, true, true, TArgs_pg_wal_replay_resume_Policy>; }
struct TArgs_pg_walfile_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_walfile_name() { return &GenericExec<TPgDirectFunc<&pg_walfile_name>, true, false, TArgs_pg_walfile_name_Policy>; }
struct TArgs_pg_walfile_name_offset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_walfile_name_offset() { return &GenericExec<TPgDirectFunc<&pg_walfile_name_offset>, true, false, TArgs_pg_walfile_name_offset_Policy>; }
struct TArgs_pg_xact_commit_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_xact_commit_timestamp() { return &GenericExec<TPgDirectFunc<&pg_xact_commit_timestamp>, true, true, TArgs_pg_xact_commit_timestamp_Policy>; }
struct TArgs_pg_xact_commit_timestamp_origin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_xact_commit_timestamp_origin() { return &GenericExec<TPgDirectFunc<&pg_xact_commit_timestamp_origin>, true, false, TArgs_pg_xact_commit_timestamp_origin_Policy>; }
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
struct TArgs_radians_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_radians() { return &GenericExec<TPgDirectFunc<&radians>, true, true, TArgs_radians_Policy>; }
struct TArgs_range_adjacent_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_adjacent_multirange() { return &GenericExec<TPgDirectFunc<&range_adjacent_multirange>, true, true, TArgs_range_adjacent_multirange_Policy>; }
struct TArgs_range_after_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_after_multirange() { return &GenericExec<TPgDirectFunc<&range_after_multirange>, true, true, TArgs_range_after_multirange_Policy>; }
struct TArgs_range_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_range_agg_finalfn() { return &GenericExec<TPgDirectFunc<&range_agg_finalfn>, false, false, TArgs_range_agg_finalfn_Policy>; }
struct TArgs_range_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_range_agg_transfn() { return &GenericExec<TPgDirectFunc<&range_agg_transfn>, false, true, TArgs_range_agg_transfn_Policy>; }
struct TArgs_range_before_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_before_multirange() { return &GenericExec<TPgDirectFunc<&range_before_multirange>, true, true, TArgs_range_before_multirange_Policy>; }
struct TArgs_range_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contained_by_multirange() { return &GenericExec<TPgDirectFunc<&range_contained_by_multirange>, true, true, TArgs_range_contained_by_multirange_Policy>; }
struct TArgs_range_contains_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contains_multirange() { return &GenericExec<TPgDirectFunc<&range_contains_multirange>, true, true, TArgs_range_contains_multirange_Policy>; }
struct TArgs_range_merge_from_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_merge_from_multirange() { return &GenericExec<TPgDirectFunc<&range_merge_from_multirange>, true, false, TArgs_range_merge_from_multirange_Policy>; }
struct TArgs_range_overlaps_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overlaps_multirange() { return &GenericExec<TPgDirectFunc<&range_overlaps_multirange>, true, true, TArgs_range_overlaps_multirange_Policy>; }
struct TArgs_range_overleft_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overleft_multirange() { return &GenericExec<TPgDirectFunc<&range_overleft_multirange>, true, true, TArgs_range_overleft_multirange_Policy>; }
struct TArgs_range_overright_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overright_multirange() { return &GenericExec<TPgDirectFunc<&range_overright_multirange>, true, true, TArgs_range_overright_multirange_Policy>; }
struct TArgs_rangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_rangesel() { return &GenericExec<TPgDirectFunc<&rangesel>, true, true, TArgs_rangesel_Policy>; }
struct TArgs_raw_array_subscript_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_raw_array_subscript_handler() { return &GenericExec<TPgDirectFunc<&raw_array_subscript_handler>, true, true, TArgs_raw_array_subscript_handler_Policy>; }
struct TArgs_regclassin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regclassin() { return &GenericExec<TPgDirectFunc<&regclassin>, true, true, TArgs_regclassin_Policy>; }
struct TArgs_regclassout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regclassout() { return &GenericExec<TPgDirectFunc<&regclassout>, true, false, TArgs_regclassout_Policy>; }
struct TArgs_regclassrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regclassrecv() { return &GenericExec<TPgDirectFunc<&regclassrecv>, true, true, TArgs_regclassrecv_Policy>; }
struct TArgs_regclasssend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regclasssend() { return &GenericExec<TPgDirectFunc<&regclasssend>, true, false, TArgs_regclasssend_Policy>; }
struct TArgs_regcollationin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regcollationin() { return &GenericExec<TPgDirectFunc<&regcollationin>, true, true, TArgs_regcollationin_Policy>; }
struct TArgs_regcollationout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regcollationout() { return &GenericExec<TPgDirectFunc<&regcollationout>, true, false, TArgs_regcollationout_Policy>; }
struct TArgs_regcollationrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regcollationrecv() { return &GenericExec<TPgDirectFunc<&regcollationrecv>, true, true, TArgs_regcollationrecv_Policy>; }
struct TArgs_regcollationsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regcollationsend() { return &GenericExec<TPgDirectFunc<&regcollationsend>, true, false, TArgs_regcollationsend_Policy>; }
struct TArgs_regconfigin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regconfigin() { return &GenericExec<TPgDirectFunc<&regconfigin>, true, true, TArgs_regconfigin_Policy>; }
struct TArgs_regconfigout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regconfigout() { return &GenericExec<TPgDirectFunc<&regconfigout>, true, false, TArgs_regconfigout_Policy>; }
struct TArgs_regconfigrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regconfigrecv() { return &GenericExec<TPgDirectFunc<&regconfigrecv>, true, true, TArgs_regconfigrecv_Policy>; }
struct TArgs_regconfigsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regconfigsend() { return &GenericExec<TPgDirectFunc<&regconfigsend>, true, false, TArgs_regconfigsend_Policy>; }
struct TArgs_regdictionaryin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regdictionaryin() { return &GenericExec<TPgDirectFunc<&regdictionaryin>, true, true, TArgs_regdictionaryin_Policy>; }
struct TArgs_regdictionaryout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regdictionaryout() { return &GenericExec<TPgDirectFunc<&regdictionaryout>, true, false, TArgs_regdictionaryout_Policy>; }
struct TArgs_regdictionaryrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regdictionaryrecv() { return &GenericExec<TPgDirectFunc<&regdictionaryrecv>, true, true, TArgs_regdictionaryrecv_Policy>; }
struct TArgs_regdictionarysend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regdictionarysend() { return &GenericExec<TPgDirectFunc<&regdictionarysend>, true, false, TArgs_regdictionarysend_Policy>; }
struct TArgs_regexp_match_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_regexp_match() { return &GenericExec<TPgDirectFunc<&regexp_match>, true, false, TArgs_regexp_match_Policy>; }
struct TArgs_regexp_match_no_flags_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_regexp_match_no_flags() { return &GenericExec<TPgDirectFunc<&regexp_match_no_flags>, true, false, TArgs_regexp_match_no_flags_Policy>; }
struct TArgs_regexp_split_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_regexp_split_to_array() { return &GenericExec<TPgDirectFunc<&regexp_split_to_array>, true, false, TArgs_regexp_split_to_array_Policy>; }
struct TArgs_regexp_split_to_array_no_flags_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_regexp_split_to_array_no_flags() { return &GenericExec<TPgDirectFunc<&regexp_split_to_array_no_flags>, true, false, TArgs_regexp_split_to_array_no_flags_Policy>; }
struct TArgs_regnamespacein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regnamespacein() { return &GenericExec<TPgDirectFunc<&regnamespacein>, true, true, TArgs_regnamespacein_Policy>; }
struct TArgs_regnamespaceout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regnamespaceout() { return &GenericExec<TPgDirectFunc<&regnamespaceout>, true, false, TArgs_regnamespaceout_Policy>; }
struct TArgs_regnamespacerecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regnamespacerecv() { return &GenericExec<TPgDirectFunc<&regnamespacerecv>, true, true, TArgs_regnamespacerecv_Policy>; }
struct TArgs_regnamespacesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regnamespacesend() { return &GenericExec<TPgDirectFunc<&regnamespacesend>, true, false, TArgs_regnamespacesend_Policy>; }
struct TArgs_regoperatorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regoperatorin() { return &GenericExec<TPgDirectFunc<&regoperatorin>, true, true, TArgs_regoperatorin_Policy>; }
struct TArgs_regoperatorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regoperatorout() { return &GenericExec<TPgDirectFunc<&regoperatorout>, true, false, TArgs_regoperatorout_Policy>; }
struct TArgs_regoperatorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regoperatorrecv() { return &GenericExec<TPgDirectFunc<&regoperatorrecv>, true, true, TArgs_regoperatorrecv_Policy>; }
struct TArgs_regoperatorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regoperatorsend() { return &GenericExec<TPgDirectFunc<&regoperatorsend>, true, false, TArgs_regoperatorsend_Policy>; }
struct TArgs_regoperin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regoperin() { return &GenericExec<TPgDirectFunc<&regoperin>, true, true, TArgs_regoperin_Policy>; }
struct TArgs_regoperout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regoperout() { return &GenericExec<TPgDirectFunc<&regoperout>, true, false, TArgs_regoperout_Policy>; }
struct TArgs_regoperrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regoperrecv() { return &GenericExec<TPgDirectFunc<&regoperrecv>, true, true, TArgs_regoperrecv_Policy>; }
struct TArgs_regopersend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regopersend() { return &GenericExec<TPgDirectFunc<&regopersend>, true, false, TArgs_regopersend_Policy>; }
struct TArgs_regprocedurein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regprocedurein() { return &GenericExec<TPgDirectFunc<&regprocedurein>, true, true, TArgs_regprocedurein_Policy>; }
struct TArgs_regprocedureout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regprocedureout() { return &GenericExec<TPgDirectFunc<&regprocedureout>, true, false, TArgs_regprocedureout_Policy>; }
struct TArgs_regprocedurerecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regprocedurerecv() { return &GenericExec<TPgDirectFunc<&regprocedurerecv>, true, true, TArgs_regprocedurerecv_Policy>; }
struct TArgs_regproceduresend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regproceduresend() { return &GenericExec<TPgDirectFunc<&regproceduresend>, true, false, TArgs_regproceduresend_Policy>; }
struct TArgs_regprocin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regprocin() { return &GenericExec<TPgDirectFunc<&regprocin>, true, true, TArgs_regprocin_Policy>; }
struct TArgs_regprocout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regprocout() { return &GenericExec<TPgDirectFunc<&regprocout>, true, false, TArgs_regprocout_Policy>; }
struct TArgs_regprocrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regprocrecv() { return &GenericExec<TPgDirectFunc<&regprocrecv>, true, true, TArgs_regprocrecv_Policy>; }
struct TArgs_regprocsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regprocsend() { return &GenericExec<TPgDirectFunc<&regprocsend>, true, false, TArgs_regprocsend_Policy>; }
struct TArgs_regrolein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regrolein() { return &GenericExec<TPgDirectFunc<&regrolein>, true, true, TArgs_regrolein_Policy>; }
struct TArgs_regroleout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regroleout() { return &GenericExec<TPgDirectFunc<&regroleout>, true, false, TArgs_regroleout_Policy>; }
struct TArgs_regrolerecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regrolerecv() { return &GenericExec<TPgDirectFunc<&regrolerecv>, true, true, TArgs_regrolerecv_Policy>; }
struct TArgs_regrolesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regrolesend() { return &GenericExec<TPgDirectFunc<&regrolesend>, true, false, TArgs_regrolesend_Policy>; }
struct TArgs_regtypein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_regtypein() { return &GenericExec<TPgDirectFunc<&regtypein>, true, true, TArgs_regtypein_Policy>; }
struct TArgs_regtypeout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regtypeout() { return &GenericExec<TPgDirectFunc<&regtypeout>, true, false, TArgs_regtypeout_Policy>; }
struct TArgs_regtyperecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regtyperecv() { return &GenericExec<TPgDirectFunc<&regtyperecv>, true, true, TArgs_regtyperecv_Policy>; }
struct TArgs_regtypesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_regtypesend() { return &GenericExec<TPgDirectFunc<&regtypesend>, true, false, TArgs_regtypesend_Policy>; }
struct TArgs_replace_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_replace_text() { return &GenericExec<TPgDirectFunc<&replace_text>, true, false, TArgs_replace_text_Policy>; }
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
struct TArgs_setseed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_setseed() { return &GenericExec<TPgDirectFunc<&setseed>, true, true, TArgs_setseed_Policy>; }
struct TArgs_similar_escape_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_similar_escape() { return &GenericExec<TPgDirectFunc<&similar_escape>, false, false, TArgs_similar_escape_Policy>; }
struct TArgs_similar_to_escape_1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_similar_to_escape_1() { return &GenericExec<TPgDirectFunc<&similar_to_escape_1>, true, false, TArgs_similar_to_escape_1_Policy>; }
struct TArgs_similar_to_escape_2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_similar_to_escape_2() { return &GenericExec<TPgDirectFunc<&similar_to_escape_2>, true, false, TArgs_similar_to_escape_2_Policy>; }
struct TArgs_split_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_split_part() { return &GenericExec<TPgDirectFunc<&split_part>, true, false, TArgs_split_part_Policy>; }
struct TArgs_string_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_string_agg_finalfn() { return &GenericExec<TPgDirectFunc<&string_agg_finalfn>, false, false, TArgs_string_agg_finalfn_Policy>; }
struct TArgs_string_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_string_agg_transfn() { return &GenericExec<TPgDirectFunc<&string_agg_transfn>, false, true, TArgs_string_agg_transfn_Policy>; }
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
struct TArgs_text_format_nv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_format_nv() { return &GenericExec<TPgDirectFunc<&text_format_nv>, false, false, TArgs_text_format_nv_Policy>; }
struct TArgs_text_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_ge() { return &GenericExec<TPgDirectFunc<&text_ge>, true, true, TArgs_text_ge_Policy>; }
struct TArgs_text_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_gt() { return &GenericExec<TPgDirectFunc<&text_gt>, true, true, TArgs_text_gt_Policy>; }
struct TArgs_text_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_larger() { return &GenericExec<TPgDirectFunc<&text_larger>, true, false, TArgs_text_larger_Policy>; }
struct TArgs_text_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_le() { return &GenericExec<TPgDirectFunc<&text_le>, true, true, TArgs_text_le_Policy>; }
struct TArgs_text_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_left() { return &GenericExec<TPgDirectFunc<&text_left>, true, false, TArgs_text_left_Policy>; }
struct TArgs_text_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_lt() { return &GenericExec<TPgDirectFunc<&text_lt>, true, true, TArgs_text_lt_Policy>; }
struct TArgs_text_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_name() { return &GenericExec<TPgDirectFunc<&text_name>, true, false, TArgs_text_name_Policy>; }
struct TArgs_text_pattern_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_ge() { return &GenericExec<TPgDirectFunc<&text_pattern_ge>, true, true, TArgs_text_pattern_ge_Policy>; }
struct TArgs_text_pattern_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_gt() { return &GenericExec<TPgDirectFunc<&text_pattern_gt>, true, true, TArgs_text_pattern_gt_Policy>; }
struct TArgs_text_pattern_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_le() { return &GenericExec<TPgDirectFunc<&text_pattern_le>, true, true, TArgs_text_pattern_le_Policy>; }
struct TArgs_text_pattern_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_lt() { return &GenericExec<TPgDirectFunc<&text_pattern_lt>, true, true, TArgs_text_pattern_lt_Policy>; }
struct TArgs_text_regclass_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_regclass() { return &GenericExec<TPgDirectFunc<&text_regclass>, true, true, TArgs_text_regclass_Policy>; }
struct TArgs_text_reverse_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_reverse() { return &GenericExec<TPgDirectFunc<&text_reverse>, true, false, TArgs_text_reverse_Policy>; }
struct TArgs_text_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_right() { return &GenericExec<TPgDirectFunc<&text_right>, true, false, TArgs_text_right_Policy>; }
struct TArgs_text_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_smaller() { return &GenericExec<TPgDirectFunc<&text_smaller>, true, false, TArgs_text_smaller_Policy>; }
struct TArgs_text_starts_with_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_starts_with() { return &GenericExec<TPgDirectFunc<&text_starts_with>, true, true, TArgs_text_starts_with_Policy>; }
struct TArgs_text_substr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_text_substr() { return &GenericExec<TPgDirectFunc<&text_substr>, true, false, TArgs_text_substr_Policy>; }
struct TArgs_text_substr_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_substr_no_len() { return &GenericExec<TPgDirectFunc<&text_substr_no_len>, true, false, TArgs_text_substr_no_len_Policy>; }
struct TArgs_text_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_to_array() { return &GenericExec<TPgDirectFunc<&text_to_array>, false, false, TArgs_text_to_array_Policy>; }
struct TArgs_text_to_array_null_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_text_to_array_null() { return &GenericExec<TPgDirectFunc<&text_to_array_null>, false, false, TArgs_text_to_array_null_Policy>; }
struct TArgs_textcat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textcat() { return &GenericExec<TPgDirectFunc<&textcat>, true, false, TArgs_textcat_Policy>; }
struct TArgs_texteq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texteq() { return &GenericExec<TPgDirectFunc<&texteq>, true, true, TArgs_texteq_Policy>; }
struct TArgs_texteqname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texteqname() { return &GenericExec<TPgDirectFunc<&texteqname>, true, true, TArgs_texteqname_Policy>; }
struct TArgs_textgename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textgename() { return &GenericExec<TPgDirectFunc<&textgename>, true, true, TArgs_textgename_Policy>; }
struct TArgs_textgtname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textgtname() { return &GenericExec<TPgDirectFunc<&textgtname>, true, true, TArgs_textgtname_Policy>; }
struct TArgs_texticregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticregexeq() { return &GenericExec<TPgDirectFunc<&texticregexeq>, true, true, TArgs_texticregexeq_Policy>; }
struct TArgs_texticregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticregexne() { return &GenericExec<TPgDirectFunc<&texticregexne>, true, true, TArgs_texticregexne_Policy>; }
struct TArgs_textin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textin() { return &GenericExec<TPgDirectFunc<&textin>, true, false, TArgs_textin_Policy>; }
struct TArgs_textlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textlen() { return &GenericExec<TPgDirectFunc<&textlen>, true, true, TArgs_textlen_Policy>; }
struct TArgs_textlename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textlename() { return &GenericExec<TPgDirectFunc<&textlename>, true, true, TArgs_textlename_Policy>; }
struct TArgs_textltname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textltname() { return &GenericExec<TPgDirectFunc<&textltname>, true, true, TArgs_textltname_Policy>; }
struct TArgs_textne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textne() { return &GenericExec<TPgDirectFunc<&textne>, true, true, TArgs_textne_Policy>; }
struct TArgs_textnename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textnename() { return &GenericExec<TPgDirectFunc<&textnename>, true, true, TArgs_textnename_Policy>; }
struct TArgs_textoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textoctetlen() { return &GenericExec<TPgDirectFunc<&textoctetlen>, true, true, TArgs_textoctetlen_Policy>; }
struct TArgs_textout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textout() { return &GenericExec<TPgDirectFunc<&textout>, true, false, TArgs_textout_Policy>; }
struct TArgs_textoverlay_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_textoverlay() { return &GenericExec<TPgDirectFunc<&textoverlay>, true, false, TArgs_textoverlay_Policy>; }
struct TArgs_textoverlay_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_textoverlay_no_len() { return &GenericExec<TPgDirectFunc<&textoverlay_no_len>, true, false, TArgs_textoverlay_no_len_Policy>; }
struct TArgs_textpos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textpos() { return &GenericExec<TPgDirectFunc<&textpos>, true, true, TArgs_textpos_Policy>; }
struct TArgs_textrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textrecv() { return &GenericExec<TPgDirectFunc<&textrecv>, true, false, TArgs_textrecv_Policy>; }
struct TArgs_textregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textregexeq() { return &GenericExec<TPgDirectFunc<&textregexeq>, true, true, TArgs_textregexeq_Policy>; }
struct TArgs_textregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textregexne() { return &GenericExec<TPgDirectFunc<&textregexne>, true, true, TArgs_textregexne_Policy>; }
struct TArgs_textregexreplace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
TExecFunc arrow_textregexreplace() { return &GenericExec<TPgDirectFunc<&textregexreplace>, true, false, TArgs_textregexreplace_Policy>; }
struct TArgs_textregexreplace_noopt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_textregexreplace_noopt() { return &GenericExec<TPgDirectFunc<&textregexreplace_noopt>, true, false, TArgs_textregexreplace_noopt_Policy>; }
struct TArgs_textregexsubstr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textregexsubstr() { return &GenericExec<TPgDirectFunc<&textregexsubstr>, true, false, TArgs_textregexsubstr_Policy>; }
struct TArgs_textsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textsend() { return &GenericExec<TPgDirectFunc<&textsend>, true, false, TArgs_textsend_Policy>; }
struct TArgs_texttoxml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_texttoxml() { return &GenericExec<TPgDirectFunc<&texttoxml>, true, false, TArgs_texttoxml_Policy>; }
struct TArgs_to_hex32_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_hex32() { return &GenericExec<TPgDirectFunc<&to_hex32>, true, false, TArgs_to_hex32_Policy>; }
struct TArgs_to_hex64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_hex64() { return &GenericExec<TPgDirectFunc<&to_hex64>, true, false, TArgs_to_hex64_Policy>; }
struct TArgs_to_regclass_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regclass() { return &GenericExec<TPgDirectFunc<&to_regclass>, true, true, TArgs_to_regclass_Policy>; }
struct TArgs_to_regcollation_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regcollation() { return &GenericExec<TPgDirectFunc<&to_regcollation>, true, true, TArgs_to_regcollation_Policy>; }
struct TArgs_to_regnamespace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regnamespace() { return &GenericExec<TPgDirectFunc<&to_regnamespace>, true, true, TArgs_to_regnamespace_Policy>; }
struct TArgs_to_regoper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regoper() { return &GenericExec<TPgDirectFunc<&to_regoper>, true, true, TArgs_to_regoper_Policy>; }
struct TArgs_to_regoperator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regoperator() { return &GenericExec<TPgDirectFunc<&to_regoperator>, true, true, TArgs_to_regoperator_Policy>; }
struct TArgs_to_regproc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regproc() { return &GenericExec<TPgDirectFunc<&to_regproc>, true, true, TArgs_to_regproc_Policy>; }
struct TArgs_to_regprocedure_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regprocedure() { return &GenericExec<TPgDirectFunc<&to_regprocedure>, true, true, TArgs_to_regprocedure_Policy>; }
struct TArgs_to_regrole_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regrole() { return &GenericExec<TPgDirectFunc<&to_regrole>, true, true, TArgs_to_regrole_Policy>; }
struct TArgs_to_regtype_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_regtype() { return &GenericExec<TPgDirectFunc<&to_regtype>, true, true, TArgs_to_regtype_Policy>; }
struct TArgs_ts_rank_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_rank_tt() { return &GenericExec<TPgDirectFunc<&ts_rank_tt>, true, true, TArgs_ts_rank_tt_Policy>; }
struct TArgs_ts_rank_ttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_ts_rank_ttf() { return &GenericExec<TPgDirectFunc<&ts_rank_ttf>, true, true, TArgs_ts_rank_ttf_Policy>; }
struct TArgs_ts_rank_wtt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_rank_wtt() { return &GenericExec<TPgDirectFunc<&ts_rank_wtt>, true, true, TArgs_ts_rank_wtt_Policy>; }
struct TArgs_ts_rank_wttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_ts_rank_wttf() { return &GenericExec<TPgDirectFunc<&ts_rank_wttf>, true, true, TArgs_ts_rank_wttf_Policy>; }
struct TArgs_ts_rankcd_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_rankcd_tt() { return &GenericExec<TPgDirectFunc<&ts_rankcd_tt>, true, true, TArgs_ts_rankcd_tt_Policy>; }
struct TArgs_ts_rankcd_ttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_ts_rankcd_ttf() { return &GenericExec<TPgDirectFunc<&ts_rankcd_ttf>, true, true, TArgs_ts_rankcd_ttf_Policy>; }
struct TArgs_ts_rankcd_wtt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_rankcd_wtt() { return &GenericExec<TPgDirectFunc<&ts_rankcd_wtt>, true, true, TArgs_ts_rankcd_wtt_Policy>; }
struct TArgs_ts_rankcd_wttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_ts_rankcd_wttf() { return &GenericExec<TPgDirectFunc<&ts_rankcd_wttf>, true, true, TArgs_ts_rankcd_wttf_Policy>; }
struct TArgs_ts_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ts_typanalyze() { return &GenericExec<TPgDirectFunc<&ts_typanalyze>, true, true, TArgs_ts_typanalyze_Policy>; }
struct TArgs_tsm_bernoulli_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_bernoulli_handler() { return &GenericExec<TPgDirectFunc<&tsm_bernoulli_handler>, true, true, TArgs_tsm_bernoulli_handler_Policy>; }
struct TArgs_tsm_system_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_system_handler() { return &GenericExec<TPgDirectFunc<&tsm_system_handler>, true, true, TArgs_tsm_system_handler_Policy>; }
struct TArgs_unicode_is_normalized_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_unicode_is_normalized() { return &GenericExec<TPgDirectFunc<&unicode_is_normalized>, true, true, TArgs_unicode_is_normalized_Policy>; }
struct TArgs_unicode_normalize_func_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_unicode_normalize_func() { return &GenericExec<TPgDirectFunc<&unicode_normalize_func>, true, false, TArgs_unicode_normalize_func_Policy>; }
struct TArgs_unique_key_recheck_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_unique_key_recheck() { return &GenericExec<TPgDirectFunc<&unique_key_recheck>, true, true, TArgs_unique_key_recheck_Policy>; }
struct TArgs_unistr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unistr() { return &GenericExec<TPgDirectFunc<&unistr>, true, false, TArgs_unistr_Policy>; }
struct TArgs_unknownin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownin() { return &GenericExec<TPgDirectFunc<&unknownin>, true, false, TArgs_unknownin_Policy>; }
struct TArgs_unknownout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownout() { return &GenericExec<TPgDirectFunc<&unknownout>, true, false, TArgs_unknownout_Policy>; }
struct TArgs_unknownrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_unknownrecv() { return &GenericExec<TPgDirectFunc<&unknownrecv>, true, false, TArgs_unknownrecv_Policy>; }
struct TArgs_unknownsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownsend() { return &GenericExec<TPgDirectFunc<&unknownsend>, true, false, TArgs_unknownsend_Policy>; }
struct TArgs_width_bucket_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_width_bucket_float8() { return &GenericExec<TPgDirectFunc<&width_bucket_float8>, true, true, TArgs_width_bucket_float8_Policy>; }
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
