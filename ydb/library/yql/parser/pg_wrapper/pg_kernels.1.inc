struct TArgs_aclcontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclcontains() { return TGenericExec<TPgDirectFunc<&aclcontains>, true, true, TArgs_aclcontains_Policy>({}); }
struct TArgs_acldefault_sql_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_acldefault_sql() { return TGenericExec<TPgDirectFunc<&acldefault_sql>, true, false, TArgs_acldefault_sql_Policy>({}); }
struct TArgs_aclinsert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclinsert() { return TGenericExec<TPgDirectFunc<&aclinsert>, true, false, TArgs_aclinsert_Policy>({}); }
struct TArgs_aclitem_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclitem_eq() { return TGenericExec<TPgDirectFunc<&aclitem_eq>, true, true, TArgs_aclitem_eq_Policy>({}); }
struct TArgs_aclitemin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_aclitemin() { return TGenericExec<TPgDirectFunc<&aclitemin>, true, false, TArgs_aclitemin_Policy>({}); }
struct TArgs_aclitemout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_aclitemout() { return TGenericExec<TPgDirectFunc<&aclitemout>, true, false, TArgs_aclitemout_Policy>({}); }
struct TArgs_aclremove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclremove() { return TGenericExec<TPgDirectFunc<&aclremove>, true, false, TArgs_aclremove_Policy>({}); }
struct TArgs_amvalidate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_amvalidate() { return TGenericExec<TPgDirectFunc<&amvalidate>, true, true, TArgs_amvalidate_Policy>({}); }
struct TArgs_array_cardinality_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_cardinality() { return TGenericExec<TPgDirectFunc<&array_cardinality>, true, true, TArgs_array_cardinality_Policy>({}); }
struct TArgs_array_dims_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_dims() { return TGenericExec<TPgDirectFunc<&array_dims>, true, false, TArgs_array_dims_Policy>({}); }
struct TArgs_array_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_eq() { return TGenericExec<TPgDirectFunc<&array_eq>, true, true, TArgs_array_eq_Policy>({}); }
struct TArgs_array_fill_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_array_fill() { return TGenericExec<TPgDirectFunc<&array_fill>, false, false, TArgs_array_fill_Policy>({}); }
struct TArgs_array_fill_with_lower_bounds_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_array_fill_with_lower_bounds() { return TGenericExec<TPgDirectFunc<&array_fill_with_lower_bounds>, false, false, TArgs_array_fill_with_lower_bounds_Policy>({}); }
struct TArgs_array_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_ge() { return TGenericExec<TPgDirectFunc<&array_ge>, true, true, TArgs_array_ge_Policy>({}); }
struct TArgs_array_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_gt() { return TGenericExec<TPgDirectFunc<&array_gt>, true, true, TArgs_array_gt_Policy>({}); }
struct TArgs_array_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_array_in() { return TGenericExec<TPgDirectFunc<&array_in>, true, false, TArgs_array_in_Policy>({}); }
struct TArgs_array_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_larger() { return TGenericExec<TPgDirectFunc<&array_larger>, true, false, TArgs_array_larger_Policy>({}); }
struct TArgs_array_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_le() { return TGenericExec<TPgDirectFunc<&array_le>, true, true, TArgs_array_le_Policy>({}); }
struct TArgs_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_length() { return TGenericExec<TPgDirectFunc<&array_length>, true, true, TArgs_array_length_Policy>({}); }
struct TArgs_array_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_lower() { return TGenericExec<TPgDirectFunc<&array_lower>, true, true, TArgs_array_lower_Policy>({}); }
struct TArgs_array_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_lt() { return TGenericExec<TPgDirectFunc<&array_lt>, true, true, TArgs_array_lt_Policy>({}); }
struct TArgs_array_ndims_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_ndims() { return TGenericExec<TPgDirectFunc<&array_ndims>, true, true, TArgs_array_ndims_Policy>({}); }
struct TArgs_array_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_ne() { return TGenericExec<TPgDirectFunc<&array_ne>, true, true, TArgs_array_ne_Policy>({}); }
struct TArgs_array_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_out() { return TGenericExec<TPgDirectFunc<&array_out>, true, false, TArgs_array_out_Policy>({}); }
struct TArgs_array_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_array_recv() { return TGenericExec<TPgDirectFunc<&array_recv>, true, false, TArgs_array_recv_Policy>({}); }
struct TArgs_array_remove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_remove() { return TGenericExec<TPgDirectFunc<&array_remove>, false, false, TArgs_array_remove_Policy>({}); }
struct TArgs_array_replace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_array_replace() { return TGenericExec<TPgDirectFunc<&array_replace>, false, false, TArgs_array_replace_Policy>({}); }
struct TArgs_array_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_send() { return TGenericExec<TPgDirectFunc<&array_send>, true, false, TArgs_array_send_Policy>({}); }
struct TArgs_array_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_smaller() { return TGenericExec<TPgDirectFunc<&array_smaller>, true, false, TArgs_array_smaller_Policy>({}); }
struct TArgs_array_to_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_to_text() { return TGenericExec<TPgDirectFunc<&array_to_text>, true, false, TArgs_array_to_text_Policy>({}); }
struct TArgs_array_to_text_null_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_array_to_text_null() { return TGenericExec<TPgDirectFunc<&array_to_text_null>, false, false, TArgs_array_to_text_null_Policy>({}); }
struct TArgs_array_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_to_tsvector() { return TGenericExec<TPgDirectFunc<&array_to_tsvector>, true, false, TArgs_array_to_tsvector_Policy>({}); }
struct TArgs_array_unnest_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_unnest_support() { return TGenericExec<TPgDirectFunc<&array_unnest_support>, true, true, TArgs_array_unnest_support_Policy>({}); }
struct TArgs_array_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_upper() { return TGenericExec<TPgDirectFunc<&array_upper>, true, true, TArgs_array_upper_Policy>({}); }
struct TArgs_arraycontained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arraycontained() { return TGenericExec<TPgDirectFunc<&arraycontained>, true, true, TArgs_arraycontained_Policy>({}); }
struct TArgs_arraycontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arraycontains() { return TGenericExec<TPgDirectFunc<&arraycontains>, true, true, TArgs_arraycontains_Policy>({}); }
struct TArgs_arrayoverlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arrayoverlap() { return TGenericExec<TPgDirectFunc<&arrayoverlap>, true, true, TArgs_arrayoverlap_Policy>({}); }
struct TArgs_binary_upgrade_create_empty_extension_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,false,true,false,false,false,false};
};
TExecFunc arrow_binary_upgrade_create_empty_extension() { return TGenericExec<TPgDirectFunc<&binary_upgrade_create_empty_extension>, false, true, TArgs_binary_upgrade_create_empty_extension_Policy>({}); }
struct TArgs_binary_upgrade_set_missing_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_binary_upgrade_set_missing_value() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_missing_value>, true, true, TArgs_binary_upgrade_set_missing_value_Policy>({}); }
struct TArgs_binary_upgrade_set_next_array_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_array_pg_type_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_array_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_array_pg_type_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_heap_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_heap_pg_class_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_heap_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_heap_pg_class_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_index_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_index_pg_class_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_index_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_index_pg_class_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_multirange_array_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_multirange_array_pg_type_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_multirange_array_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_multirange_array_pg_type_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_multirange_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_multirange_pg_type_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_multirange_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_multirange_pg_type_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_pg_authid_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_authid_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_authid_oid>, true, true, TArgs_binary_upgrade_set_next_pg_authid_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_pg_enum_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_enum_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_enum_oid>, true, true, TArgs_binary_upgrade_set_next_pg_enum_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_pg_type_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_pg_type_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_pg_type_oid>, true, true, TArgs_binary_upgrade_set_next_pg_type_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_next_toast_pg_class_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_next_toast_pg_class_oid() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_next_toast_pg_class_oid>, true, true, TArgs_binary_upgrade_set_next_toast_pg_class_oid_Policy>({}); }
struct TArgs_binary_upgrade_set_record_init_privs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_binary_upgrade_set_record_init_privs() { return TGenericExec<TPgDirectFunc<&binary_upgrade_set_record_init_privs>, true, true, TArgs_binary_upgrade_set_record_init_privs_Policy>({}); }
struct TArgs_bool_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bool_int4() { return TGenericExec<TPgDirectFunc<&bool_int4>, true, true, TArgs_bool_int4_Policy>({}); }
struct TArgs_brin_inclusion_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_inclusion_add_value() { return TGenericExec<TPgDirectFunc<&brin_inclusion_add_value>, true, true, TArgs_brin_inclusion_add_value_Policy>({}); }
struct TArgs_brin_inclusion_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_inclusion_consistent() { return TGenericExec<TPgDirectFunc<&brin_inclusion_consistent>, true, true, TArgs_brin_inclusion_consistent_Policy>({}); }
struct TArgs_brin_inclusion_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_inclusion_opcinfo() { return TGenericExec<TPgDirectFunc<&brin_inclusion_opcinfo>, true, true, TArgs_brin_inclusion_opcinfo_Policy>({}); }
struct TArgs_brin_inclusion_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_inclusion_union() { return TGenericExec<TPgDirectFunc<&brin_inclusion_union>, true, true, TArgs_brin_inclusion_union_Policy>({}); }
struct TArgs_btarraycmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btarraycmp() { return TGenericExec<TPgDirectFunc<&btarraycmp>, true, true, TArgs_btarraycmp_Policy>({}); }
struct TArgs_btequalimage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btequalimage() { return TGenericExec<TPgDirectFunc<&btequalimage>, true, true, TArgs_btequalimage_Policy>({}); }
struct TArgs_bthandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bthandler() { return TGenericExec<TPgDirectFunc<&bthandler>, true, true, TArgs_bthandler_Policy>({}); }
struct TArgs_btnametextcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btnametextcmp() { return TGenericExec<TPgDirectFunc<&btnametextcmp>, true, true, TArgs_btnametextcmp_Policy>({}); }
struct TArgs_bttext_pattern_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttext_pattern_cmp() { return TGenericExec<TPgDirectFunc<&bttext_pattern_cmp>, true, true, TArgs_bttext_pattern_cmp_Policy>({}); }
struct TArgs_bttext_pattern_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bttext_pattern_sortsupport() { return TGenericExec<TPgDirectFunc<&bttext_pattern_sortsupport>, true, true, TArgs_bttext_pattern_sortsupport_Policy>({}); }
struct TArgs_bttextcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttextcmp() { return TGenericExec<TPgDirectFunc<&bttextcmp>, true, true, TArgs_bttextcmp_Policy>({}); }
struct TArgs_bttextnamecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttextnamecmp() { return TGenericExec<TPgDirectFunc<&bttextnamecmp>, true, true, TArgs_bttextnamecmp_Policy>({}); }
struct TArgs_bttextsortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bttextsortsupport() { return TGenericExec<TPgDirectFunc<&bttextsortsupport>, true, true, TArgs_bttextsortsupport_Policy>({}); }
struct TArgs_btvarstrequalimage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btvarstrequalimage() { return TGenericExec<TPgDirectFunc<&btvarstrequalimage>, true, true, TArgs_btvarstrequalimage_Policy>({}); }
struct TArgs_byteaGetBit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_byteaGetBit() { return TGenericExec<TPgDirectFunc<&byteaGetBit>, true, true, TArgs_byteaGetBit_Policy>({}); }
struct TArgs_byteaGetByte_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_byteaGetByte() { return TGenericExec<TPgDirectFunc<&byteaGetByte>, true, true, TArgs_byteaGetByte_Policy>({}); }
struct TArgs_byteaSetBit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_byteaSetBit() { return TGenericExec<TPgDirectFunc<&byteaSetBit>, true, false, TArgs_byteaSetBit_Policy>({}); }
struct TArgs_byteaSetByte_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_byteaSetByte() { return TGenericExec<TPgDirectFunc<&byteaSetByte>, true, false, TArgs_byteaSetByte_Policy>({}); }
struct TArgs_bytea_bit_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bytea_bit_count() { return TGenericExec<TPgDirectFunc<&bytea_bit_count>, true, true, TArgs_bytea_bit_count_Policy>({}); }
struct TArgs_bytea_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytea_sortsupport() { return TGenericExec<TPgDirectFunc<&bytea_sortsupport>, true, true, TArgs_bytea_sortsupport_Policy>({}); }
struct TArgs_bytea_string_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytea_string_agg_finalfn() { return TGenericExec<TPgDirectFunc<&bytea_string_agg_finalfn>, false, false, TArgs_bytea_string_agg_finalfn_Policy>({}); }
struct TArgs_bytea_string_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_bytea_string_agg_transfn() { return TGenericExec<TPgDirectFunc<&bytea_string_agg_transfn>, false, true, TArgs_bytea_string_agg_transfn_Policy>({}); }
struct TArgs_bytea_substr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bytea_substr() { return TGenericExec<TPgDirectFunc<&bytea_substr>, true, false, TArgs_bytea_substr_Policy>({}); }
struct TArgs_bytea_substr_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bytea_substr_no_len() { return TGenericExec<TPgDirectFunc<&bytea_substr_no_len>, true, false, TArgs_bytea_substr_no_len_Policy>({}); }
struct TArgs_byteacat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteacat() { return TGenericExec<TPgDirectFunc<&byteacat>, true, false, TArgs_byteacat_Policy>({}); }
struct TArgs_byteacmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteacmp() { return TGenericExec<TPgDirectFunc<&byteacmp>, true, true, TArgs_byteacmp_Policy>({}); }
struct TArgs_byteaeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteaeq() { return TGenericExec<TPgDirectFunc<&byteaeq>, true, true, TArgs_byteaeq_Policy>({}); }
struct TArgs_byteage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteage() { return TGenericExec<TPgDirectFunc<&byteage>, true, true, TArgs_byteage_Policy>({}); }
struct TArgs_byteagt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteagt() { return TGenericExec<TPgDirectFunc<&byteagt>, true, true, TArgs_byteagt_Policy>({}); }
struct TArgs_byteain_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteain() { return TGenericExec<TPgDirectFunc<&byteain>, true, false, TArgs_byteain_Policy>({}); }
struct TArgs_byteale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteale() { return TGenericExec<TPgDirectFunc<&byteale>, true, true, TArgs_byteale_Policy>({}); }
struct TArgs_bytealt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealt() { return TGenericExec<TPgDirectFunc<&bytealt>, true, true, TArgs_bytealt_Policy>({}); }
struct TArgs_byteane_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteane() { return TGenericExec<TPgDirectFunc<&byteane>, true, true, TArgs_byteane_Policy>({}); }
struct TArgs_byteaoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteaoctetlen() { return TGenericExec<TPgDirectFunc<&byteaoctetlen>, true, true, TArgs_byteaoctetlen_Policy>({}); }
struct TArgs_byteaout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteaout() { return TGenericExec<TPgDirectFunc<&byteaout>, true, false, TArgs_byteaout_Policy>({}); }
struct TArgs_byteaoverlay_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_byteaoverlay() { return TGenericExec<TPgDirectFunc<&byteaoverlay>, true, false, TArgs_byteaoverlay_Policy>({}); }
struct TArgs_byteaoverlay_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_byteaoverlay_no_len() { return TGenericExec<TPgDirectFunc<&byteaoverlay_no_len>, true, false, TArgs_byteaoverlay_no_len_Policy>({}); }
struct TArgs_byteapos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteapos() { return TGenericExec<TPgDirectFunc<&byteapos>, true, true, TArgs_byteapos_Policy>({}); }
struct TArgs_bytearecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bytearecv() { return TGenericExec<TPgDirectFunc<&bytearecv>, true, false, TArgs_bytearecv_Policy>({}); }
struct TArgs_byteasend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteasend() { return TGenericExec<TPgDirectFunc<&byteasend>, true, false, TArgs_byteasend_Policy>({}); }
struct TArgs_cursor_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,true,true,true,false};
};
TExecFunc arrow_cursor_to_xml() { return TGenericExec<TPgDirectFunc<&cursor_to_xml>, true, false, TArgs_cursor_to_xml_Policy>({}); }
struct TArgs_cursor_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_cursor_to_xmlschema() { return TGenericExec<TPgDirectFunc<&cursor_to_xmlschema>, true, false, TArgs_cursor_to_xmlschema_Policy>({}); }
struct TArgs_database_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xml() { return TGenericExec<TPgDirectFunc<&database_to_xml>, true, false, TArgs_database_to_xml_Policy>({}); }
struct TArgs_database_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xml_and_xmlschema() { return TGenericExec<TPgDirectFunc<&database_to_xml_and_xmlschema>, true, false, TArgs_database_to_xml_and_xmlschema_Policy>({}); }
struct TArgs_database_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_database_to_xmlschema() { return TGenericExec<TPgDirectFunc<&database_to_xmlschema>, true, false, TArgs_database_to_xmlschema_Policy>({}); }
struct TArgs_date_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp() { return TGenericExec<TPgDirectFunc<&date_cmp>, true, true, TArgs_date_cmp_Policy>({}); }
struct TArgs_date_cmp_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp_timestamp() { return TGenericExec<TPgDirectFunc<&date_cmp_timestamp>, true, true, TArgs_date_cmp_timestamp_Policy>({}); }
struct TArgs_date_cmp_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_cmp_timestamptz() { return TGenericExec<TPgDirectFunc<&date_cmp_timestamptz>, true, true, TArgs_date_cmp_timestamptz_Policy>({}); }
struct TArgs_date_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq() { return TGenericExec<TPgDirectFunc<&date_eq>, true, true, TArgs_date_eq_Policy>({}); }
struct TArgs_date_eq_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq_timestamp() { return TGenericExec<TPgDirectFunc<&date_eq_timestamp>, true, true, TArgs_date_eq_timestamp_Policy>({}); }
struct TArgs_date_eq_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_eq_timestamptz() { return TGenericExec<TPgDirectFunc<&date_eq_timestamptz>, true, true, TArgs_date_eq_timestamptz_Policy>({}); }
struct TArgs_date_finite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_finite() { return TGenericExec<TPgDirectFunc<&date_finite>, true, true, TArgs_date_finite_Policy>({}); }
struct TArgs_date_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge() { return TGenericExec<TPgDirectFunc<&date_ge>, true, true, TArgs_date_ge_Policy>({}); }
struct TArgs_date_ge_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge_timestamp() { return TGenericExec<TPgDirectFunc<&date_ge_timestamp>, true, true, TArgs_date_ge_timestamp_Policy>({}); }
struct TArgs_date_ge_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ge_timestamptz() { return TGenericExec<TPgDirectFunc<&date_ge_timestamptz>, true, true, TArgs_date_ge_timestamptz_Policy>({}); }
struct TArgs_date_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt() { return TGenericExec<TPgDirectFunc<&date_gt>, true, true, TArgs_date_gt_Policy>({}); }
struct TArgs_date_gt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt_timestamp() { return TGenericExec<TPgDirectFunc<&date_gt_timestamp>, true, true, TArgs_date_gt_timestamp_Policy>({}); }
struct TArgs_date_gt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_gt_timestamptz() { return TGenericExec<TPgDirectFunc<&date_gt_timestamptz>, true, true, TArgs_date_gt_timestamptz_Policy>({}); }
struct TArgs_date_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_date_in() { return TGenericExec<TPgDirectFunc<&date_in>, true, true, TArgs_date_in_Policy>({}); }
struct TArgs_date_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_larger() { return TGenericExec<TPgDirectFunc<&date_larger>, true, true, TArgs_date_larger_Policy>({}); }
struct TArgs_date_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le() { return TGenericExec<TPgDirectFunc<&date_le>, true, true, TArgs_date_le_Policy>({}); }
struct TArgs_date_le_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le_timestamp() { return TGenericExec<TPgDirectFunc<&date_le_timestamp>, true, true, TArgs_date_le_timestamp_Policy>({}); }
struct TArgs_date_le_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_le_timestamptz() { return TGenericExec<TPgDirectFunc<&date_le_timestamptz>, true, true, TArgs_date_le_timestamptz_Policy>({}); }
struct TArgs_date_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt() { return TGenericExec<TPgDirectFunc<&date_lt>, true, true, TArgs_date_lt_Policy>({}); }
struct TArgs_date_lt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt_timestamp() { return TGenericExec<TPgDirectFunc<&date_lt_timestamp>, true, true, TArgs_date_lt_timestamp_Policy>({}); }
struct TArgs_date_lt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_lt_timestamptz() { return TGenericExec<TPgDirectFunc<&date_lt_timestamptz>, true, true, TArgs_date_lt_timestamptz_Policy>({}); }
struct TArgs_date_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_mi() { return TGenericExec<TPgDirectFunc<&date_mi>, true, true, TArgs_date_mi_Policy>({}); }
struct TArgs_date_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_date_mi_interval() { return TGenericExec<TPgDirectFunc<&date_mi_interval>, true, true, TArgs_date_mi_interval_Policy>({}); }
struct TArgs_date_mii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_mii() { return TGenericExec<TPgDirectFunc<&date_mii>, true, true, TArgs_date_mii_Policy>({}); }
struct TArgs_date_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne() { return TGenericExec<TPgDirectFunc<&date_ne>, true, true, TArgs_date_ne_Policy>({}); }
struct TArgs_date_ne_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne_timestamp() { return TGenericExec<TPgDirectFunc<&date_ne_timestamp>, true, true, TArgs_date_ne_timestamp_Policy>({}); }
struct TArgs_date_ne_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_ne_timestamptz() { return TGenericExec<TPgDirectFunc<&date_ne_timestamptz>, true, true, TArgs_date_ne_timestamptz_Policy>({}); }
struct TArgs_date_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_out() { return TGenericExec<TPgDirectFunc<&date_out>, true, false, TArgs_date_out_Policy>({}); }
struct TArgs_date_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_date_pl_interval() { return TGenericExec<TPgDirectFunc<&date_pl_interval>, true, true, TArgs_date_pl_interval_Policy>({}); }
struct TArgs_date_pli_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_pli() { return TGenericExec<TPgDirectFunc<&date_pli>, true, true, TArgs_date_pli_Policy>({}); }
struct TArgs_date_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_recv() { return TGenericExec<TPgDirectFunc<&date_recv>, true, true, TArgs_date_recv_Policy>({}); }
struct TArgs_date_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_send() { return TGenericExec<TPgDirectFunc<&date_send>, true, false, TArgs_date_send_Policy>({}); }
struct TArgs_date_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_date_smaller() { return TGenericExec<TPgDirectFunc<&date_smaller>, true, true, TArgs_date_smaller_Policy>({}); }
struct TArgs_date_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_sortsupport() { return TGenericExec<TPgDirectFunc<&date_sortsupport>, true, true, TArgs_date_sortsupport_Policy>({}); }
struct TArgs_date_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_timestamp() { return TGenericExec<TPgDirectFunc<&date_timestamp>, true, true, TArgs_date_timestamp_Policy>({}); }
struct TArgs_date_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_date_timestamptz() { return TGenericExec<TPgDirectFunc<&date_timestamptz>, true, true, TArgs_date_timestamptz_Policy>({}); }
struct TArgs_datetime_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datetime_timestamp() { return TGenericExec<TPgDirectFunc<&datetime_timestamp>, true, true, TArgs_datetime_timestamp_Policy>({}); }
struct TArgs_datetimetz_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_datetimetz_timestamptz() { return TGenericExec<TPgDirectFunc<&datetimetz_timestamptz>, true, true, TArgs_datetimetz_timestamptz_Policy>({}); }
struct TArgs_dsimple_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsimple_init() { return TGenericExec<TPgDirectFunc<&dsimple_init>, true, true, TArgs_dsimple_init_Policy>({}); }
struct TArgs_dsimple_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dsimple_lexize() { return TGenericExec<TPgDirectFunc<&dsimple_lexize>, true, true, TArgs_dsimple_lexize_Policy>({}); }
struct TArgs_dtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi8() { return TGenericExec<TPgDirectFunc<&dtoi8>, true, true, TArgs_dtoi8_Policy>({}); }
struct TArgs_extract_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_date() { return TGenericExec<TPgDirectFunc<&extract_date>, true, false, TArgs_extract_date_Policy>({}); }
struct TArgs_extract_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_time() { return TGenericExec<TPgDirectFunc<&extract_time>, true, false, TArgs_extract_time_Policy>({}); }
struct TArgs_extract_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_extract_timetz() { return TGenericExec<TPgDirectFunc<&extract_timetz>, true, false, TArgs_extract_timetz_Policy>({}); }
struct TArgs_ftoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi8() { return TGenericExec<TPgDirectFunc<&ftoi8>, true, true, TArgs_ftoi8_Policy>({}); }
struct TArgs_generate_series_int4_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int4_support() { return TGenericExec<TPgDirectFunc<&generate_series_int4_support>, true, true, TArgs_generate_series_int4_support_Policy>({}); }
struct TArgs_generate_series_int8_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int8_support() { return TGenericExec<TPgDirectFunc<&generate_series_int8_support>, true, true, TArgs_generate_series_int8_support_Policy>({}); }
struct TArgs_get_current_ts_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_get_current_ts_config() { return TGenericExec<TPgDirectFunc<&get_current_ts_config>, true, true, TArgs_get_current_ts_config_Policy>({}); }
struct TArgs_gin_cmp_prefix_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_gin_cmp_prefix() { return TGenericExec<TPgDirectFunc<&gin_cmp_prefix>, true, true, TArgs_gin_cmp_prefix_Policy>({}); }
struct TArgs_gin_cmp_tslexeme_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_gin_cmp_tslexeme() { return TGenericExec<TPgDirectFunc<&gin_cmp_tslexeme>, true, true, TArgs_gin_cmp_tslexeme_Policy>({}); }
struct TArgs_gin_compare_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_gin_compare_jsonb() { return TGenericExec<TPgDirectFunc<&gin_compare_jsonb>, true, true, TArgs_gin_compare_jsonb_Policy>({}); }
struct TArgs_gin_consistent_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_consistent_jsonb() { return TGenericExec<TPgDirectFunc<&gin_consistent_jsonb>, true, true, TArgs_gin_consistent_jsonb_Policy>({}); }
struct TArgs_gin_consistent_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_consistent_jsonb_path() { return TGenericExec<TPgDirectFunc<&gin_consistent_jsonb_path>, true, true, TArgs_gin_consistent_jsonb_path_Policy>({}); }
struct TArgs_gin_extract_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_jsonb() { return TGenericExec<TPgDirectFunc<&gin_extract_jsonb>, true, true, TArgs_gin_extract_jsonb_Policy>({}); }
struct TArgs_gin_extract_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_jsonb_path() { return TGenericExec<TPgDirectFunc<&gin_extract_jsonb_path>, true, true, TArgs_gin_extract_jsonb_path_Policy>({}); }
struct TArgs_gin_extract_jsonb_query_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_jsonb_query() { return TGenericExec<TPgDirectFunc<&gin_extract_jsonb_query>, true, true, TArgs_gin_extract_jsonb_query_Policy>({}); }
struct TArgs_gin_extract_jsonb_query_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_jsonb_query_path() { return TGenericExec<TPgDirectFunc<&gin_extract_jsonb_query_path>, true, true, TArgs_gin_extract_jsonb_query_path_Policy>({}); }
struct TArgs_gin_extract_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery() { return TGenericExec<TPgDirectFunc<&gin_extract_tsquery>, true, true, TArgs_gin_extract_tsquery_Policy>({}); }
struct TArgs_gin_extract_tsquery_5args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery_5args() { return TGenericExec<TPgDirectFunc<&gin_extract_tsquery_5args>, true, true, TArgs_gin_extract_tsquery_5args_Policy>({}); }
struct TArgs_gin_extract_tsquery_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_tsquery_oldsig() { return TGenericExec<TPgDirectFunc<&gin_extract_tsquery_oldsig>, true, true, TArgs_gin_extract_tsquery_oldsig_Policy>({}); }
struct TArgs_gin_extract_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_tsvector() { return TGenericExec<TPgDirectFunc<&gin_extract_tsvector>, true, true, TArgs_gin_extract_tsvector_Policy>({}); }
struct TArgs_gin_extract_tsvector_2args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_gin_extract_tsvector_2args() { return TGenericExec<TPgDirectFunc<&gin_extract_tsvector_2args>, true, true, TArgs_gin_extract_tsvector_2args_Policy>({}); }
struct TArgs_gin_triconsistent_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_triconsistent_jsonb() { return TGenericExec<TPgDirectFunc<&gin_triconsistent_jsonb>, true, true, TArgs_gin_triconsistent_jsonb_Policy>({}); }
struct TArgs_gin_triconsistent_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_triconsistent_jsonb_path() { return TGenericExec<TPgDirectFunc<&gin_triconsistent_jsonb_path>, true, true, TArgs_gin_triconsistent_jsonb_path_Policy>({}); }
struct TArgs_gin_tsquery_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent() { return TGenericExec<TPgDirectFunc<&gin_tsquery_consistent>, true, true, TArgs_gin_tsquery_consistent_Policy>({}); }
struct TArgs_gin_tsquery_consistent_6args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 6> IsFixedArg = {true,true,false,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent_6args() { return TGenericExec<TPgDirectFunc<&gin_tsquery_consistent_6args>, true, true, TArgs_gin_tsquery_consistent_6args_Policy>({}); }
struct TArgs_gin_tsquery_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_consistent_oldsig() { return TGenericExec<TPgDirectFunc<&gin_tsquery_consistent_oldsig>, true, true, TArgs_gin_tsquery_consistent_oldsig_Policy>({}); }
struct TArgs_gin_tsquery_triconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_tsquery_triconsistent() { return TGenericExec<TPgDirectFunc<&gin_tsquery_triconsistent>, true, true, TArgs_gin_tsquery_triconsistent_Policy>({}); }
struct TArgs_gtsquery_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gtsquery_compress() { return TGenericExec<TPgDirectFunc<&gtsquery_compress>, true, true, TArgs_gtsquery_compress_Policy>({}); }
struct TArgs_gtsquery_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gtsquery_consistent() { return TGenericExec<TPgDirectFunc<&gtsquery_consistent>, true, true, TArgs_gtsquery_consistent_Policy>({}); }
struct TArgs_gtsquery_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_gtsquery_consistent_oldsig() { return TGenericExec<TPgDirectFunc<&gtsquery_consistent_oldsig>, true, true, TArgs_gtsquery_consistent_oldsig_Policy>({}); }
struct TArgs_gtsquery_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gtsquery_penalty() { return TGenericExec<TPgDirectFunc<&gtsquery_penalty>, true, true, TArgs_gtsquery_penalty_Policy>({}); }
struct TArgs_gtsquery_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsquery_picksplit() { return TGenericExec<TPgDirectFunc<&gtsquery_picksplit>, true, true, TArgs_gtsquery_picksplit_Policy>({}); }
struct TArgs_gtsquery_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gtsquery_same() { return TGenericExec<TPgDirectFunc<&gtsquery_same>, true, true, TArgs_gtsquery_same_Policy>({}); }
struct TArgs_gtsquery_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsquery_union() { return TGenericExec<TPgDirectFunc<&gtsquery_union>, true, true, TArgs_gtsquery_union_Policy>({}); }
struct TArgs_has_any_column_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_any_column_privilege_id() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_id>, true, true, TArgs_has_any_column_privilege_id_Policy>({}); }
struct TArgs_has_any_column_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_any_column_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_id_id>, true, true, TArgs_has_any_column_privilege_id_id_Policy>({}); }
struct TArgs_has_any_column_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_any_column_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_id_name>, true, true, TArgs_has_any_column_privilege_id_name_Policy>({}); }
struct TArgs_has_any_column_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_any_column_privilege_name() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_name>, true, true, TArgs_has_any_column_privilege_name_Policy>({}); }
struct TArgs_has_any_column_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_any_column_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_name_id>, true, true, TArgs_has_any_column_privilege_name_id_Policy>({}); }
struct TArgs_has_any_column_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_any_column_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_any_column_privilege_name_name>, true, true, TArgs_has_any_column_privilege_name_name_Policy>({}); }
struct TArgs_has_column_privilege_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_column_privilege_id_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_attnum>, true, true, TArgs_has_column_privilege_id_attnum_Policy>({}); }
struct TArgs_has_column_privilege_id_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_has_column_privilege_id_id_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_id_attnum>, true, true, TArgs_has_column_privilege_id_id_attnum_Policy>({}); }
struct TArgs_has_column_privilege_id_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,false,false};
};
TExecFunc arrow_has_column_privilege_id_id_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_id_name>, true, true, TArgs_has_column_privilege_id_id_name_Policy>({}); }
struct TArgs_has_column_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_column_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_name>, true, true, TArgs_has_column_privilege_id_name_Policy>({}); }
struct TArgs_has_column_privilege_id_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,true,false};
};
TExecFunc arrow_has_column_privilege_id_name_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_name_attnum>, true, true, TArgs_has_column_privilege_id_name_attnum_Policy>({}); }
struct TArgs_has_column_privilege_id_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_has_column_privilege_id_name_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_id_name_name>, true, true, TArgs_has_column_privilege_id_name_name_Policy>({}); }
struct TArgs_has_column_privilege_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_column_privilege_name_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_attnum>, true, true, TArgs_has_column_privilege_name_attnum_Policy>({}); }
struct TArgs_has_column_privilege_name_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_has_column_privilege_name_id_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_id_attnum>, true, true, TArgs_has_column_privilege_name_id_attnum_Policy>({}); }
struct TArgs_has_column_privilege_name_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,false,false};
};
TExecFunc arrow_has_column_privilege_name_id_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_id_name>, true, true, TArgs_has_column_privilege_name_id_name_Policy>({}); }
struct TArgs_has_column_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_column_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_name>, true, true, TArgs_has_column_privilege_name_name_Policy>({}); }
struct TArgs_has_column_privilege_name_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,false};
};
TExecFunc arrow_has_column_privilege_name_name_attnum() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_name_attnum>, true, true, TArgs_has_column_privilege_name_name_attnum_Policy>({}); }
struct TArgs_has_column_privilege_name_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
TExecFunc arrow_has_column_privilege_name_name_name() { return TGenericExec<TPgDirectFunc<&has_column_privilege_name_name_name>, true, true, TArgs_has_column_privilege_name_name_name_Policy>({}); }
struct TArgs_has_database_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_database_privilege_id() { return TGenericExec<TPgDirectFunc<&has_database_privilege_id>, true, true, TArgs_has_database_privilege_id_Policy>({}); }
struct TArgs_has_database_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_database_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_database_privilege_id_id>, true, true, TArgs_has_database_privilege_id_id_Policy>({}); }
struct TArgs_has_database_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_database_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_database_privilege_id_name>, true, true, TArgs_has_database_privilege_id_name_Policy>({}); }
struct TArgs_has_database_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_database_privilege_name() { return TGenericExec<TPgDirectFunc<&has_database_privilege_name>, true, true, TArgs_has_database_privilege_name_Policy>({}); }
struct TArgs_has_database_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_database_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_database_privilege_name_id>, true, true, TArgs_has_database_privilege_name_id_Policy>({}); }
struct TArgs_has_database_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_database_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_database_privilege_name_name>, true, true, TArgs_has_database_privilege_name_name_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_id_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_name_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_id_Policy>({}); }
struct TArgs_has_foreign_data_wrapper_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_name_Policy>({}); }
struct TArgs_has_function_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_function_privilege_id() { return TGenericExec<TPgDirectFunc<&has_function_privilege_id>, true, true, TArgs_has_function_privilege_id_Policy>({}); }
struct TArgs_has_function_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_function_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_function_privilege_id_id>, true, true, TArgs_has_function_privilege_id_id_Policy>({}); }
struct TArgs_has_function_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_function_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_function_privilege_id_name>, true, true, TArgs_has_function_privilege_id_name_Policy>({}); }
struct TArgs_has_function_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_function_privilege_name() { return TGenericExec<TPgDirectFunc<&has_function_privilege_name>, true, true, TArgs_has_function_privilege_name_Policy>({}); }
struct TArgs_has_function_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_function_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_function_privilege_name_id>, true, true, TArgs_has_function_privilege_name_id_Policy>({}); }
struct TArgs_has_function_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_function_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_function_privilege_name_name>, true, true, TArgs_has_function_privilege_name_name_Policy>({}); }
struct TArgs_has_language_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_language_privilege_id() { return TGenericExec<TPgDirectFunc<&has_language_privilege_id>, true, true, TArgs_has_language_privilege_id_Policy>({}); }
struct TArgs_has_language_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_language_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_language_privilege_id_id>, true, true, TArgs_has_language_privilege_id_id_Policy>({}); }
struct TArgs_has_language_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_language_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_language_privilege_id_name>, true, true, TArgs_has_language_privilege_id_name_Policy>({}); }
struct TArgs_has_language_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_language_privilege_name() { return TGenericExec<TPgDirectFunc<&has_language_privilege_name>, true, true, TArgs_has_language_privilege_name_Policy>({}); }
struct TArgs_has_language_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_language_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_language_privilege_name_id>, true, true, TArgs_has_language_privilege_name_id_Policy>({}); }
struct TArgs_has_language_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_language_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_language_privilege_name_name>, true, true, TArgs_has_language_privilege_name_name_Policy>({}); }
struct TArgs_has_schema_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_schema_privilege_id() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_id>, true, true, TArgs_has_schema_privilege_id_Policy>({}); }
struct TArgs_has_schema_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_schema_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_id_id>, true, true, TArgs_has_schema_privilege_id_id_Policy>({}); }
struct TArgs_has_schema_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_schema_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_id_name>, true, true, TArgs_has_schema_privilege_id_name_Policy>({}); }
struct TArgs_has_schema_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_schema_privilege_name() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_name>, true, true, TArgs_has_schema_privilege_name_Policy>({}); }
struct TArgs_has_schema_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_schema_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_name_id>, true, true, TArgs_has_schema_privilege_name_id_Policy>({}); }
struct TArgs_has_schema_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_schema_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_schema_privilege_name_name>, true, true, TArgs_has_schema_privilege_name_name_Policy>({}); }
struct TArgs_has_sequence_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_sequence_privilege_id() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_id>, true, true, TArgs_has_sequence_privilege_id_Policy>({}); }
struct TArgs_has_sequence_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_sequence_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_id_id>, true, true, TArgs_has_sequence_privilege_id_id_Policy>({}); }
struct TArgs_has_sequence_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_sequence_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_id_name>, true, true, TArgs_has_sequence_privilege_id_name_Policy>({}); }
struct TArgs_has_sequence_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_sequence_privilege_name() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_name>, true, true, TArgs_has_sequence_privilege_name_Policy>({}); }
struct TArgs_has_sequence_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_sequence_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_name_id>, true, true, TArgs_has_sequence_privilege_name_id_Policy>({}); }
struct TArgs_has_sequence_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_sequence_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_sequence_privilege_name_name>, true, true, TArgs_has_sequence_privilege_name_name_Policy>({}); }
struct TArgs_has_server_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_server_privilege_id() { return TGenericExec<TPgDirectFunc<&has_server_privilege_id>, true, true, TArgs_has_server_privilege_id_Policy>({}); }
struct TArgs_has_server_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_server_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_server_privilege_id_id>, true, true, TArgs_has_server_privilege_id_id_Policy>({}); }
struct TArgs_has_server_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_server_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_server_privilege_id_name>, true, true, TArgs_has_server_privilege_id_name_Policy>({}); }
struct TArgs_has_server_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_server_privilege_name() { return TGenericExec<TPgDirectFunc<&has_server_privilege_name>, true, true, TArgs_has_server_privilege_name_Policy>({}); }
struct TArgs_has_server_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_server_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_server_privilege_name_id>, true, true, TArgs_has_server_privilege_name_id_Policy>({}); }
struct TArgs_has_server_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_server_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_server_privilege_name_name>, true, true, TArgs_has_server_privilege_name_name_Policy>({}); }
struct TArgs_has_table_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_table_privilege_id() { return TGenericExec<TPgDirectFunc<&has_table_privilege_id>, true, true, TArgs_has_table_privilege_id_Policy>({}); }
struct TArgs_has_table_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_table_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_table_privilege_id_id>, true, true, TArgs_has_table_privilege_id_id_Policy>({}); }
struct TArgs_has_table_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_table_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_table_privilege_id_name>, true, true, TArgs_has_table_privilege_id_name_Policy>({}); }
struct TArgs_has_table_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_table_privilege_name() { return TGenericExec<TPgDirectFunc<&has_table_privilege_name>, true, true, TArgs_has_table_privilege_name_Policy>({}); }
struct TArgs_has_table_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_table_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_table_privilege_name_id>, true, true, TArgs_has_table_privilege_name_id_Policy>({}); }
struct TArgs_has_table_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_table_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_table_privilege_name_name>, true, true, TArgs_has_table_privilege_name_name_Policy>({}); }
struct TArgs_has_tablespace_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_tablespace_privilege_id() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_id>, true, true, TArgs_has_tablespace_privilege_id_Policy>({}); }
struct TArgs_has_tablespace_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_tablespace_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_id_id>, true, true, TArgs_has_tablespace_privilege_id_id_Policy>({}); }
struct TArgs_has_tablespace_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_tablespace_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_id_name>, true, true, TArgs_has_tablespace_privilege_id_name_Policy>({}); }
struct TArgs_has_tablespace_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_tablespace_privilege_name() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_name>, true, true, TArgs_has_tablespace_privilege_name_Policy>({}); }
struct TArgs_has_tablespace_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_tablespace_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_name_id>, true, true, TArgs_has_tablespace_privilege_name_id_Policy>({}); }
struct TArgs_has_tablespace_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_tablespace_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_tablespace_privilege_name_name>, true, true, TArgs_has_tablespace_privilege_name_name_Policy>({}); }
struct TArgs_has_type_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_type_privilege_id() { return TGenericExec<TPgDirectFunc<&has_type_privilege_id>, true, true, TArgs_has_type_privilege_id_Policy>({}); }
struct TArgs_has_type_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_type_privilege_id_id() { return TGenericExec<TPgDirectFunc<&has_type_privilege_id_id>, true, true, TArgs_has_type_privilege_id_id_Policy>({}); }
struct TArgs_has_type_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_type_privilege_id_name() { return TGenericExec<TPgDirectFunc<&has_type_privilege_id_name>, true, true, TArgs_has_type_privilege_id_name_Policy>({}); }
struct TArgs_has_type_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_type_privilege_name() { return TGenericExec<TPgDirectFunc<&has_type_privilege_name>, true, true, TArgs_has_type_privilege_name_Policy>({}); }
struct TArgs_has_type_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_type_privilege_name_id() { return TGenericExec<TPgDirectFunc<&has_type_privilege_name_id>, true, true, TArgs_has_type_privilege_name_id_Policy>({}); }
struct TArgs_has_type_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_type_privilege_name_name() { return TGenericExec<TPgDirectFunc<&has_type_privilege_name_name>, true, true, TArgs_has_type_privilege_name_name_Policy>({}); }
struct TArgs_hash_aclitem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_aclitem() { return TGenericExec<TPgDirectFunc<&hash_aclitem>, true, true, TArgs_hash_aclitem_Policy>({}); }
struct TArgs_hash_aclitem_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_aclitem_extended() { return TGenericExec<TPgDirectFunc<&hash_aclitem_extended>, true, true, TArgs_hash_aclitem_extended_Policy>({}); }
struct TArgs_hash_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_array() { return TGenericExec<TPgDirectFunc<&hash_array>, true, true, TArgs_hash_array_Policy>({}); }
struct TArgs_hash_array_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_array_extended() { return TGenericExec<TPgDirectFunc<&hash_array_extended>, true, true, TArgs_hash_array_extended_Policy>({}); }
struct TArgs_i2toi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2toi4() { return TGenericExec<TPgDirectFunc<&i2toi4>, true, true, TArgs_i2toi4_Policy>({}); }
struct TArgs_i4toi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4toi2() { return TGenericExec<TPgDirectFunc<&i4toi2>, true, true, TArgs_i4toi2_Policy>({}); }
struct TArgs_i8tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tod() { return TGenericExec<TPgDirectFunc<&i8tod>, true, true, TArgs_i8tod_Policy>({}); }
struct TArgs_i8tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tof() { return TGenericExec<TPgDirectFunc<&i8tof>, true, true, TArgs_i8tof_Policy>({}); }
struct TArgs_i8tooid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tooid() { return TGenericExec<TPgDirectFunc<&i8tooid>, true, true, TArgs_i8tooid_Policy>({}); }
struct TArgs_in_range_date_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_date_interval() { return TGenericExec<TPgDirectFunc<&in_range_date_interval>, true, true, TArgs_in_range_date_interval_Policy>({}); }
struct TArgs_in_range_int2_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int2() { return TGenericExec<TPgDirectFunc<&in_range_int2_int2>, true, true, TArgs_in_range_int2_int2_Policy>({}); }
struct TArgs_in_range_int2_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int4() { return TGenericExec<TPgDirectFunc<&in_range_int2_int4>, true, true, TArgs_in_range_int2_int4_Policy>({}); }
struct TArgs_in_range_int2_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int2_int8() { return TGenericExec<TPgDirectFunc<&in_range_int2_int8>, true, true, TArgs_in_range_int2_int8_Policy>({}); }
struct TArgs_in_range_int4_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int2() { return TGenericExec<TPgDirectFunc<&in_range_int4_int2>, true, true, TArgs_in_range_int4_int2_Policy>({}); }
struct TArgs_in_range_int4_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int4() { return TGenericExec<TPgDirectFunc<&in_range_int4_int4>, true, true, TArgs_in_range_int4_int4_Policy>({}); }
struct TArgs_in_range_int4_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int4_int8() { return TGenericExec<TPgDirectFunc<&in_range_int4_int8>, true, true, TArgs_in_range_int4_int8_Policy>({}); }
struct TArgs_in_range_int8_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int8_int8() { return TGenericExec<TPgDirectFunc<&in_range_int8_int8>, true, true, TArgs_in_range_int8_int8_Policy>({}); }
struct TArgs_in_range_time_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_time_interval() { return TGenericExec<TPgDirectFunc<&in_range_time_interval>, true, true, TArgs_in_range_time_interval_Policy>({}); }
struct TArgs_in_range_timetz_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_timetz_interval() { return TGenericExec<TPgDirectFunc<&in_range_timetz_interval>, true, true, TArgs_in_range_timetz_interval_Policy>({}); }
struct TArgs_inet_spg_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_choose() { return TGenericExec<TPgDirectFunc<&inet_spg_choose>, true, true, TArgs_inet_spg_choose_Policy>({}); }
struct TArgs_inet_spg_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_config() { return TGenericExec<TPgDirectFunc<&inet_spg_config>, true, true, TArgs_inet_spg_config_Policy>({}); }
struct TArgs_inet_spg_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_inner_consistent() { return TGenericExec<TPgDirectFunc<&inet_spg_inner_consistent>, true, true, TArgs_inet_spg_inner_consistent_Policy>({}); }
struct TArgs_inet_spg_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_leaf_consistent() { return TGenericExec<TPgDirectFunc<&inet_spg_leaf_consistent>, true, true, TArgs_inet_spg_leaf_consistent_Policy>({}); }
struct TArgs_inet_spg_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_picksplit() { return TGenericExec<TPgDirectFunc<&inet_spg_picksplit>, true, true, TArgs_inet_spg_picksplit_Policy>({}); }
struct TArgs_int24div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24div() { return TGenericExec<TPgDirectFunc<&int24div>, true, true, TArgs_int24div_Policy>({}); }
struct TArgs_int24eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24eq() { return TGenericExec<TPgDirectFunc<&int24eq>, true, true, TArgs_int24eq_Policy>({}); }
struct TArgs_int24ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24ge() { return TGenericExec<TPgDirectFunc<&int24ge>, true, true, TArgs_int24ge_Policy>({}); }
struct TArgs_int24gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24gt() { return TGenericExec<TPgDirectFunc<&int24gt>, true, true, TArgs_int24gt_Policy>({}); }
struct TArgs_int24le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24le() { return TGenericExec<TPgDirectFunc<&int24le>, true, true, TArgs_int24le_Policy>({}); }
struct TArgs_int24lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24lt() { return TGenericExec<TPgDirectFunc<&int24lt>, true, true, TArgs_int24lt_Policy>({}); }
struct TArgs_int24mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24mi() { return TGenericExec<TPgDirectFunc<&int24mi>, true, true, TArgs_int24mi_Policy>({}); }
struct TArgs_int24mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24mul() { return TGenericExec<TPgDirectFunc<&int24mul>, true, true, TArgs_int24mul_Policy>({}); }
struct TArgs_int24ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24ne() { return TGenericExec<TPgDirectFunc<&int24ne>, true, true, TArgs_int24ne_Policy>({}); }
struct TArgs_int24pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int24pl() { return TGenericExec<TPgDirectFunc<&int24pl>, true, true, TArgs_int24pl_Policy>({}); }
struct TArgs_int28_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int28() { return TGenericExec<TPgDirectFunc<&int28>, true, true, TArgs_int28_Policy>({}); }
struct TArgs_int28div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28div() { return TGenericExec<TPgDirectFunc<&int28div>, true, true, TArgs_int28div_Policy>({}); }
struct TArgs_int28eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28eq() { return TGenericExec<TPgDirectFunc<&int28eq>, true, true, TArgs_int28eq_Policy>({}); }
struct TArgs_int28ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28ge() { return TGenericExec<TPgDirectFunc<&int28ge>, true, true, TArgs_int28ge_Policy>({}); }
struct TArgs_int28gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28gt() { return TGenericExec<TPgDirectFunc<&int28gt>, true, true, TArgs_int28gt_Policy>({}); }
struct TArgs_int28le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28le() { return TGenericExec<TPgDirectFunc<&int28le>, true, true, TArgs_int28le_Policy>({}); }
struct TArgs_int28lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28lt() { return TGenericExec<TPgDirectFunc<&int28lt>, true, true, TArgs_int28lt_Policy>({}); }
struct TArgs_int28mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28mi() { return TGenericExec<TPgDirectFunc<&int28mi>, true, true, TArgs_int28mi_Policy>({}); }
struct TArgs_int28mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28mul() { return TGenericExec<TPgDirectFunc<&int28mul>, true, true, TArgs_int28mul_Policy>({}); }
struct TArgs_int28ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28ne() { return TGenericExec<TPgDirectFunc<&int28ne>, true, true, TArgs_int28ne_Policy>({}); }
struct TArgs_int28pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28pl() { return TGenericExec<TPgDirectFunc<&int28pl>, true, true, TArgs_int28pl_Policy>({}); }
struct TArgs_int2abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2abs() { return TGenericExec<TPgDirectFunc<&int2abs>, true, true, TArgs_int2abs_Policy>({}); }
struct TArgs_int2and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2and() { return TGenericExec<TPgDirectFunc<&int2and>, true, true, TArgs_int2and_Policy>({}); }
struct TArgs_int2div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2div() { return TGenericExec<TPgDirectFunc<&int2div>, true, true, TArgs_int2div_Policy>({}); }
struct TArgs_int2eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2eq() { return TGenericExec<TPgDirectFunc<&int2eq>, true, true, TArgs_int2eq_Policy>({}); }
struct TArgs_int2ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2ge() { return TGenericExec<TPgDirectFunc<&int2ge>, true, true, TArgs_int2ge_Policy>({}); }
struct TArgs_int2gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2gt() { return TGenericExec<TPgDirectFunc<&int2gt>, true, true, TArgs_int2gt_Policy>({}); }
struct TArgs_int2in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2in() { return TGenericExec<TPgDirectFunc<&int2in>, true, true, TArgs_int2in_Policy>({}); }
struct TArgs_int2larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2larger() { return TGenericExec<TPgDirectFunc<&int2larger>, true, true, TArgs_int2larger_Policy>({}); }
struct TArgs_int2le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2le() { return TGenericExec<TPgDirectFunc<&int2le>, true, true, TArgs_int2le_Policy>({}); }
struct TArgs_int2lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2lt() { return TGenericExec<TPgDirectFunc<&int2lt>, true, true, TArgs_int2lt_Policy>({}); }
struct TArgs_int2mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mi() { return TGenericExec<TPgDirectFunc<&int2mi>, true, true, TArgs_int2mi_Policy>({}); }
struct TArgs_int2mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mod() { return TGenericExec<TPgDirectFunc<&int2mod>, true, true, TArgs_int2mod_Policy>({}); }
struct TArgs_int2mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2mul() { return TGenericExec<TPgDirectFunc<&int2mul>, true, true, TArgs_int2mul_Policy>({}); }
struct TArgs_int2ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2ne() { return TGenericExec<TPgDirectFunc<&int2ne>, true, true, TArgs_int2ne_Policy>({}); }
struct TArgs_int2not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2not() { return TGenericExec<TPgDirectFunc<&int2not>, true, true, TArgs_int2not_Policy>({}); }
struct TArgs_int2or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2or() { return TGenericExec<TPgDirectFunc<&int2or>, true, true, TArgs_int2or_Policy>({}); }
struct TArgs_int2out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2out() { return TGenericExec<TPgDirectFunc<&int2out>, true, false, TArgs_int2out_Policy>({}); }
struct TArgs_int2pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2pl() { return TGenericExec<TPgDirectFunc<&int2pl>, true, true, TArgs_int2pl_Policy>({}); }
struct TArgs_int2recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2recv() { return TGenericExec<TPgDirectFunc<&int2recv>, true, true, TArgs_int2recv_Policy>({}); }
struct TArgs_int2send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2send() { return TGenericExec<TPgDirectFunc<&int2send>, true, false, TArgs_int2send_Policy>({}); }
struct TArgs_int2shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2shl() { return TGenericExec<TPgDirectFunc<&int2shl>, true, true, TArgs_int2shl_Policy>({}); }
struct TArgs_int2shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2shr() { return TGenericExec<TPgDirectFunc<&int2shr>, true, true, TArgs_int2shr_Policy>({}); }
struct TArgs_int2smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2smaller() { return TGenericExec<TPgDirectFunc<&int2smaller>, true, true, TArgs_int2smaller_Policy>({}); }
struct TArgs_int2um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2um() { return TGenericExec<TPgDirectFunc<&int2um>, true, true, TArgs_int2um_Policy>({}); }
struct TArgs_int2up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2up() { return TGenericExec<TPgDirectFunc<&int2up>, true, true, TArgs_int2up_Policy>({}); }
struct TArgs_int2vectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorin() { return TGenericExec<TPgDirectFunc<&int2vectorin>, true, false, TArgs_int2vectorin_Policy>({}); }
struct TArgs_int2vectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorout() { return TGenericExec<TPgDirectFunc<&int2vectorout>, true, false, TArgs_int2vectorout_Policy>({}); }
struct TArgs_int2vectorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2vectorrecv() { return TGenericExec<TPgDirectFunc<&int2vectorrecv>, true, false, TArgs_int2vectorrecv_Policy>({}); }
struct TArgs_int2vectorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2vectorsend() { return TGenericExec<TPgDirectFunc<&int2vectorsend>, true, false, TArgs_int2vectorsend_Policy>({}); }
struct TArgs_int2xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2xor() { return TGenericExec<TPgDirectFunc<&int2xor>, true, true, TArgs_int2xor_Policy>({}); }
struct TArgs_int42div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42div() { return TGenericExec<TPgDirectFunc<&int42div>, true, true, TArgs_int42div_Policy>({}); }
struct TArgs_int42eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42eq() { return TGenericExec<TPgDirectFunc<&int42eq>, true, true, TArgs_int42eq_Policy>({}); }
struct TArgs_int42ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42ge() { return TGenericExec<TPgDirectFunc<&int42ge>, true, true, TArgs_int42ge_Policy>({}); }
struct TArgs_int42gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42gt() { return TGenericExec<TPgDirectFunc<&int42gt>, true, true, TArgs_int42gt_Policy>({}); }
struct TArgs_int42le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42le() { return TGenericExec<TPgDirectFunc<&int42le>, true, true, TArgs_int42le_Policy>({}); }
struct TArgs_int42lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42lt() { return TGenericExec<TPgDirectFunc<&int42lt>, true, true, TArgs_int42lt_Policy>({}); }
struct TArgs_int42mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42mi() { return TGenericExec<TPgDirectFunc<&int42mi>, true, true, TArgs_int42mi_Policy>({}); }
struct TArgs_int42mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42mul() { return TGenericExec<TPgDirectFunc<&int42mul>, true, true, TArgs_int42mul_Policy>({}); }
struct TArgs_int42ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42ne() { return TGenericExec<TPgDirectFunc<&int42ne>, true, true, TArgs_int42ne_Policy>({}); }
struct TArgs_int42pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int42pl() { return TGenericExec<TPgDirectFunc<&int42pl>, true, true, TArgs_int42pl_Policy>({}); }
struct TArgs_int48_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int48() { return TGenericExec<TPgDirectFunc<&int48>, true, true, TArgs_int48_Policy>({}); }
struct TArgs_int48div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48div() { return TGenericExec<TPgDirectFunc<&int48div>, true, true, TArgs_int48div_Policy>({}); }
struct TArgs_int48eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48eq() { return TGenericExec<TPgDirectFunc<&int48eq>, true, true, TArgs_int48eq_Policy>({}); }
struct TArgs_int48ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48ge() { return TGenericExec<TPgDirectFunc<&int48ge>, true, true, TArgs_int48ge_Policy>({}); }
struct TArgs_int48gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48gt() { return TGenericExec<TPgDirectFunc<&int48gt>, true, true, TArgs_int48gt_Policy>({}); }
struct TArgs_int48le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48le() { return TGenericExec<TPgDirectFunc<&int48le>, true, true, TArgs_int48le_Policy>({}); }
struct TArgs_int48lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48lt() { return TGenericExec<TPgDirectFunc<&int48lt>, true, true, TArgs_int48lt_Policy>({}); }
struct TArgs_int48mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48mi() { return TGenericExec<TPgDirectFunc<&int48mi>, true, true, TArgs_int48mi_Policy>({}); }
struct TArgs_int48mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48mul() { return TGenericExec<TPgDirectFunc<&int48mul>, true, true, TArgs_int48mul_Policy>({}); }
struct TArgs_int48ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48ne() { return TGenericExec<TPgDirectFunc<&int48ne>, true, true, TArgs_int48ne_Policy>({}); }
struct TArgs_int48pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48pl() { return TGenericExec<TPgDirectFunc<&int48pl>, true, true, TArgs_int48pl_Policy>({}); }
struct TArgs_int4_bool_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4_bool() { return TGenericExec<TPgDirectFunc<&int4_bool>, true, true, TArgs_int4_bool_Policy>({}); }
struct TArgs_int4abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4abs() { return TGenericExec<TPgDirectFunc<&int4abs>, true, true, TArgs_int4abs_Policy>({}); }
struct TArgs_int4and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4and() { return TGenericExec<TPgDirectFunc<&int4and>, true, true, TArgs_int4and_Policy>({}); }
struct TArgs_int4div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4div() { return TGenericExec<TPgDirectFunc<&int4div>, true, true, TArgs_int4div_Policy>({}); }
struct TArgs_int4eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4eq() { return TGenericExec<TPgDirectFunc<&int4eq>, true, true, TArgs_int4eq_Policy>({}); }
struct TArgs_int4gcd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4gcd() { return TGenericExec<TPgDirectFunc<&int4gcd>, true, true, TArgs_int4gcd_Policy>({}); }
struct TArgs_int4ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4ge() { return TGenericExec<TPgDirectFunc<&int4ge>, true, true, TArgs_int4ge_Policy>({}); }
struct TArgs_int4gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4gt() { return TGenericExec<TPgDirectFunc<&int4gt>, true, true, TArgs_int4gt_Policy>({}); }
struct TArgs_int4in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int4in() { return TGenericExec<TPgDirectFunc<&int4in>, true, true, TArgs_int4in_Policy>({}); }
struct TArgs_int4inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4inc() { return TGenericExec<TPgDirectFunc<&int4inc>, true, true, TArgs_int4inc_Policy>({}); }
struct TArgs_int4larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4larger() { return TGenericExec<TPgDirectFunc<&int4larger>, true, true, TArgs_int4larger_Policy>({}); }
struct TArgs_int4lcm_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4lcm() { return TGenericExec<TPgDirectFunc<&int4lcm>, true, true, TArgs_int4lcm_Policy>({}); }
struct TArgs_int4le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4le() { return TGenericExec<TPgDirectFunc<&int4le>, true, true, TArgs_int4le_Policy>({}); }
struct TArgs_int4lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4lt() { return TGenericExec<TPgDirectFunc<&int4lt>, true, true, TArgs_int4lt_Policy>({}); }
struct TArgs_int4mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mi() { return TGenericExec<TPgDirectFunc<&int4mi>, true, true, TArgs_int4mi_Policy>({}); }
struct TArgs_int4mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mod() { return TGenericExec<TPgDirectFunc<&int4mod>, true, true, TArgs_int4mod_Policy>({}); }
struct TArgs_int4mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4mul() { return TGenericExec<TPgDirectFunc<&int4mul>, true, true, TArgs_int4mul_Policy>({}); }
struct TArgs_int4ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4ne() { return TGenericExec<TPgDirectFunc<&int4ne>, true, true, TArgs_int4ne_Policy>({}); }
struct TArgs_int4not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4not() { return TGenericExec<TPgDirectFunc<&int4not>, true, true, TArgs_int4not_Policy>({}); }
struct TArgs_int4or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4or() { return TGenericExec<TPgDirectFunc<&int4or>, true, true, TArgs_int4or_Policy>({}); }
struct TArgs_int4out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4out() { return TGenericExec<TPgDirectFunc<&int4out>, true, false, TArgs_int4out_Policy>({}); }
struct TArgs_int4pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4pl() { return TGenericExec<TPgDirectFunc<&int4pl>, true, true, TArgs_int4pl_Policy>({}); }
struct TArgs_int4recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4recv() { return TGenericExec<TPgDirectFunc<&int4recv>, true, true, TArgs_int4recv_Policy>({}); }
struct TArgs_int4send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4send() { return TGenericExec<TPgDirectFunc<&int4send>, true, false, TArgs_int4send_Policy>({}); }
struct TArgs_int4shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4shl() { return TGenericExec<TPgDirectFunc<&int4shl>, true, true, TArgs_int4shl_Policy>({}); }
struct TArgs_int4shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4shr() { return TGenericExec<TPgDirectFunc<&int4shr>, true, true, TArgs_int4shr_Policy>({}); }
struct TArgs_int4smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4smaller() { return TGenericExec<TPgDirectFunc<&int4smaller>, true, true, TArgs_int4smaller_Policy>({}); }
struct TArgs_int4um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4um() { return TGenericExec<TPgDirectFunc<&int4um>, true, true, TArgs_int4um_Policy>({}); }
struct TArgs_int4up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4up() { return TGenericExec<TPgDirectFunc<&int4up>, true, true, TArgs_int4up_Policy>({}); }
struct TArgs_int4xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4xor() { return TGenericExec<TPgDirectFunc<&int4xor>, true, true, TArgs_int4xor_Policy>({}); }
struct TArgs_int82_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int82() { return TGenericExec<TPgDirectFunc<&int82>, true, true, TArgs_int82_Policy>({}); }
struct TArgs_int82div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82div() { return TGenericExec<TPgDirectFunc<&int82div>, true, true, TArgs_int82div_Policy>({}); }
struct TArgs_int82eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82eq() { return TGenericExec<TPgDirectFunc<&int82eq>, true, true, TArgs_int82eq_Policy>({}); }
struct TArgs_int82ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82ge() { return TGenericExec<TPgDirectFunc<&int82ge>, true, true, TArgs_int82ge_Policy>({}); }
struct TArgs_int82gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82gt() { return TGenericExec<TPgDirectFunc<&int82gt>, true, true, TArgs_int82gt_Policy>({}); }
struct TArgs_int82le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82le() { return TGenericExec<TPgDirectFunc<&int82le>, true, true, TArgs_int82le_Policy>({}); }
struct TArgs_int82lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82lt() { return TGenericExec<TPgDirectFunc<&int82lt>, true, true, TArgs_int82lt_Policy>({}); }
struct TArgs_int82mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82mi() { return TGenericExec<TPgDirectFunc<&int82mi>, true, true, TArgs_int82mi_Policy>({}); }
struct TArgs_int82mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82mul() { return TGenericExec<TPgDirectFunc<&int82mul>, true, true, TArgs_int82mul_Policy>({}); }
struct TArgs_int82ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82ne() { return TGenericExec<TPgDirectFunc<&int82ne>, true, true, TArgs_int82ne_Policy>({}); }
struct TArgs_int82pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82pl() { return TGenericExec<TPgDirectFunc<&int82pl>, true, true, TArgs_int82pl_Policy>({}); }
struct TArgs_int84_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int84() { return TGenericExec<TPgDirectFunc<&int84>, true, true, TArgs_int84_Policy>({}); }
struct TArgs_int84div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84div() { return TGenericExec<TPgDirectFunc<&int84div>, true, true, TArgs_int84div_Policy>({}); }
struct TArgs_int84eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84eq() { return TGenericExec<TPgDirectFunc<&int84eq>, true, true, TArgs_int84eq_Policy>({}); }
struct TArgs_int84ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84ge() { return TGenericExec<TPgDirectFunc<&int84ge>, true, true, TArgs_int84ge_Policy>({}); }
struct TArgs_int84gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84gt() { return TGenericExec<TPgDirectFunc<&int84gt>, true, true, TArgs_int84gt_Policy>({}); }
struct TArgs_int84le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84le() { return TGenericExec<TPgDirectFunc<&int84le>, true, true, TArgs_int84le_Policy>({}); }
struct TArgs_int84lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84lt() { return TGenericExec<TPgDirectFunc<&int84lt>, true, true, TArgs_int84lt_Policy>({}); }
struct TArgs_int84mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84mi() { return TGenericExec<TPgDirectFunc<&int84mi>, true, true, TArgs_int84mi_Policy>({}); }
struct TArgs_int84mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84mul() { return TGenericExec<TPgDirectFunc<&int84mul>, true, true, TArgs_int84mul_Policy>({}); }
struct TArgs_int84ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84ne() { return TGenericExec<TPgDirectFunc<&int84ne>, true, true, TArgs_int84ne_Policy>({}); }
struct TArgs_int84pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84pl() { return TGenericExec<TPgDirectFunc<&int84pl>, true, true, TArgs_int84pl_Policy>({}); }
struct TArgs_int8abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8abs() { return TGenericExec<TPgDirectFunc<&int8abs>, true, true, TArgs_int8abs_Policy>({}); }
struct TArgs_int8and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8and() { return TGenericExec<TPgDirectFunc<&int8and>, true, true, TArgs_int8and_Policy>({}); }
struct TArgs_int8dec_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8dec() { return TGenericExec<TPgDirectFunc<&int8dec>, true, true, TArgs_int8dec_Policy>({}); }
struct TArgs_int8dec_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8dec_any() { return TGenericExec<TPgDirectFunc<&int8dec_any>, true, true, TArgs_int8dec_any_Policy>({}); }
struct TArgs_int8div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8div() { return TGenericExec<TPgDirectFunc<&int8div>, true, true, TArgs_int8div_Policy>({}); }
struct TArgs_int8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8eq() { return TGenericExec<TPgDirectFunc<&int8eq>, true, true, TArgs_int8eq_Policy>({}); }
struct TArgs_int8gcd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8gcd() { return TGenericExec<TPgDirectFunc<&int8gcd>, true, true, TArgs_int8gcd_Policy>({}); }
struct TArgs_int8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8ge() { return TGenericExec<TPgDirectFunc<&int8ge>, true, true, TArgs_int8ge_Policy>({}); }
struct TArgs_int8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8gt() { return TGenericExec<TPgDirectFunc<&int8gt>, true, true, TArgs_int8gt_Policy>({}); }
struct TArgs_int8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int8in() { return TGenericExec<TPgDirectFunc<&int8in>, true, true, TArgs_int8in_Policy>({}); }
struct TArgs_int8inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8inc() { return TGenericExec<TPgDirectFunc<&int8inc>, true, true, TArgs_int8inc_Policy>({}); }
struct TArgs_int8inc_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8inc_any() { return TGenericExec<TPgDirectFunc<&int8inc_any>, true, true, TArgs_int8inc_any_Policy>({}); }
struct TArgs_int8inc_float8_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_int8inc_float8_float8() { return TGenericExec<TPgDirectFunc<&int8inc_float8_float8>, true, true, TArgs_int8inc_float8_float8_Policy>({}); }
struct TArgs_int8larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8larger() { return TGenericExec<TPgDirectFunc<&int8larger>, true, true, TArgs_int8larger_Policy>({}); }
struct TArgs_int8lcm_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8lcm() { return TGenericExec<TPgDirectFunc<&int8lcm>, true, true, TArgs_int8lcm_Policy>({}); }
struct TArgs_int8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8le() { return TGenericExec<TPgDirectFunc<&int8le>, true, true, TArgs_int8le_Policy>({}); }
struct TArgs_int8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8lt() { return TGenericExec<TPgDirectFunc<&int8lt>, true, true, TArgs_int8lt_Policy>({}); }
struct TArgs_int8mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mi() { return TGenericExec<TPgDirectFunc<&int8mi>, true, true, TArgs_int8mi_Policy>({}); }
struct TArgs_int8mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mod() { return TGenericExec<TPgDirectFunc<&int8mod>, true, true, TArgs_int8mod_Policy>({}); }
struct TArgs_int8mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mul() { return TGenericExec<TPgDirectFunc<&int8mul>, true, true, TArgs_int8mul_Policy>({}); }
struct TArgs_int8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8ne() { return TGenericExec<TPgDirectFunc<&int8ne>, true, true, TArgs_int8ne_Policy>({}); }
struct TArgs_int8not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8not() { return TGenericExec<TPgDirectFunc<&int8not>, true, true, TArgs_int8not_Policy>({}); }
struct TArgs_int8or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8or() { return TGenericExec<TPgDirectFunc<&int8or>, true, true, TArgs_int8or_Policy>({}); }
struct TArgs_int8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8out() { return TGenericExec<TPgDirectFunc<&int8out>, true, false, TArgs_int8out_Policy>({}); }
struct TArgs_int8pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8pl() { return TGenericExec<TPgDirectFunc<&int8pl>, true, true, TArgs_int8pl_Policy>({}); }
struct TArgs_int8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8recv() { return TGenericExec<TPgDirectFunc<&int8recv>, true, true, TArgs_int8recv_Policy>({}); }
struct TArgs_int8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8send() { return TGenericExec<TPgDirectFunc<&int8send>, true, false, TArgs_int8send_Policy>({}); }
struct TArgs_int8shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8shl() { return TGenericExec<TPgDirectFunc<&int8shl>, true, true, TArgs_int8shl_Policy>({}); }
struct TArgs_int8shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8shr() { return TGenericExec<TPgDirectFunc<&int8shr>, true, true, TArgs_int8shr_Policy>({}); }
struct TArgs_int8smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8smaller() { return TGenericExec<TPgDirectFunc<&int8smaller>, true, true, TArgs_int8smaller_Policy>({}); }
struct TArgs_int8um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8um() { return TGenericExec<TPgDirectFunc<&int8um>, true, true, TArgs_int8um_Policy>({}); }
struct TArgs_int8up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8up() { return TGenericExec<TPgDirectFunc<&int8up>, true, true, TArgs_int8up_Policy>({}); }
struct TArgs_int8xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8xor() { return TGenericExec<TPgDirectFunc<&int8xor>, true, true, TArgs_int8xor_Policy>({}); }
struct TArgs_interval_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_time() { return TGenericExec<TPgDirectFunc<&interval_time>, true, true, TArgs_interval_time_Policy>({}); }
struct TArgs_json_string_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_string_to_tsvector() { return TGenericExec<TPgDirectFunc<&json_string_to_tsvector>, true, false, TArgs_json_string_to_tsvector_Policy>({}); }
struct TArgs_json_string_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_json_string_to_tsvector_byid() { return TGenericExec<TPgDirectFunc<&json_string_to_tsvector_byid>, true, false, TArgs_json_string_to_tsvector_byid_Policy>({}); }
struct TArgs_json_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_json_to_tsvector() { return TGenericExec<TPgDirectFunc<&json_to_tsvector>, true, false, TArgs_json_to_tsvector_Policy>({}); }
struct TArgs_json_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_json_to_tsvector_byid() { return TGenericExec<TPgDirectFunc<&json_to_tsvector_byid>, true, false, TArgs_json_to_tsvector_byid_Policy>({}); }
struct TArgs_jsonb_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_cmp() { return TGenericExec<TPgDirectFunc<&jsonb_cmp>, true, true, TArgs_jsonb_cmp_Policy>({}); }
struct TArgs_jsonb_contained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_contained() { return TGenericExec<TPgDirectFunc<&jsonb_contained>, true, true, TArgs_jsonb_contained_Policy>({}); }
struct TArgs_jsonb_contains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_contains() { return TGenericExec<TPgDirectFunc<&jsonb_contains>, true, true, TArgs_jsonb_contains_Policy>({}); }
struct TArgs_jsonb_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_eq() { return TGenericExec<TPgDirectFunc<&jsonb_eq>, true, true, TArgs_jsonb_eq_Policy>({}); }
struct TArgs_jsonb_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists() { return TGenericExec<TPgDirectFunc<&jsonb_exists>, true, true, TArgs_jsonb_exists_Policy>({}); }
struct TArgs_jsonb_exists_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists_all() { return TGenericExec<TPgDirectFunc<&jsonb_exists_all>, true, true, TArgs_jsonb_exists_all_Policy>({}); }
struct TArgs_jsonb_exists_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_exists_any() { return TGenericExec<TPgDirectFunc<&jsonb_exists_any>, true, true, TArgs_jsonb_exists_any_Policy>({}); }
struct TArgs_jsonb_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_ge() { return TGenericExec<TPgDirectFunc<&jsonb_ge>, true, true, TArgs_jsonb_ge_Policy>({}); }
struct TArgs_jsonb_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_gt() { return TGenericExec<TPgDirectFunc<&jsonb_gt>, true, true, TArgs_jsonb_gt_Policy>({}); }
struct TArgs_jsonb_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_hash() { return TGenericExec<TPgDirectFunc<&jsonb_hash>, true, true, TArgs_jsonb_hash_Policy>({}); }
struct TArgs_jsonb_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_jsonb_hash_extended() { return TGenericExec<TPgDirectFunc<&jsonb_hash_extended>, true, true, TArgs_jsonb_hash_extended_Policy>({}); }
struct TArgs_jsonb_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_le() { return TGenericExec<TPgDirectFunc<&jsonb_le>, true, true, TArgs_jsonb_le_Policy>({}); }
struct TArgs_jsonb_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_lt() { return TGenericExec<TPgDirectFunc<&jsonb_lt>, true, true, TArgs_jsonb_lt_Policy>({}); }
struct TArgs_jsonb_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_ne() { return TGenericExec<TPgDirectFunc<&jsonb_ne>, true, true, TArgs_jsonb_ne_Policy>({}); }
struct TArgs_jsonb_path_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_exists() { return TGenericExec<TPgDirectFunc<&jsonb_path_exists>, true, true, TArgs_jsonb_path_exists_Policy>({}); }
struct TArgs_jsonb_path_exists_opr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_path_exists_opr() { return TGenericExec<TPgDirectFunc<&jsonb_path_exists_opr>, true, true, TArgs_jsonb_path_exists_opr_Policy>({}); }
struct TArgs_jsonb_path_exists_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_exists_tz() { return TGenericExec<TPgDirectFunc<&jsonb_path_exists_tz>, true, true, TArgs_jsonb_path_exists_tz_Policy>({}); }
struct TArgs_jsonb_path_match_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_match() { return TGenericExec<TPgDirectFunc<&jsonb_path_match>, true, true, TArgs_jsonb_path_match_Policy>({}); }
struct TArgs_jsonb_path_match_opr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_path_match_opr() { return TGenericExec<TPgDirectFunc<&jsonb_path_match_opr>, true, true, TArgs_jsonb_path_match_opr_Policy>({}); }
struct TArgs_jsonb_path_match_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_match_tz() { return TGenericExec<TPgDirectFunc<&jsonb_path_match_tz>, true, true, TArgs_jsonb_path_match_tz_Policy>({}); }
struct TArgs_jsonb_path_query_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_array() { return TGenericExec<TPgDirectFunc<&jsonb_path_query_array>, true, false, TArgs_jsonb_path_query_array_Policy>({}); }
struct TArgs_jsonb_path_query_array_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_array_tz() { return TGenericExec<TPgDirectFunc<&jsonb_path_query_array_tz>, true, false, TArgs_jsonb_path_query_array_tz_Policy>({}); }
struct TArgs_jsonb_path_query_first_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_first() { return TGenericExec<TPgDirectFunc<&jsonb_path_query_first>, true, false, TArgs_jsonb_path_query_first_Policy>({}); }
struct TArgs_jsonb_path_query_first_tz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_jsonb_path_query_first_tz() { return TGenericExec<TPgDirectFunc<&jsonb_path_query_first_tz>, true, false, TArgs_jsonb_path_query_first_tz_Policy>({}); }
struct TArgs_jsonb_string_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_string_to_tsvector() { return TGenericExec<TPgDirectFunc<&jsonb_string_to_tsvector>, true, false, TArgs_jsonb_string_to_tsvector_Policy>({}); }
struct TArgs_jsonb_string_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_jsonb_string_to_tsvector_byid() { return TGenericExec<TPgDirectFunc<&jsonb_string_to_tsvector_byid>, true, false, TArgs_jsonb_string_to_tsvector_byid_Policy>({}); }
struct TArgs_jsonb_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_to_tsvector() { return TGenericExec<TPgDirectFunc<&jsonb_to_tsvector>, true, false, TArgs_jsonb_to_tsvector_Policy>({}); }
struct TArgs_jsonb_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_jsonb_to_tsvector_byid() { return TGenericExec<TPgDirectFunc<&jsonb_to_tsvector_byid>, true, false, TArgs_jsonb_to_tsvector_byid_Policy>({}); }
struct TArgs_make_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_make_date() { return TGenericExec<TPgDirectFunc<&make_date>, true, true, TArgs_make_date_Policy>({}); }
struct TArgs_make_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_make_time() { return TGenericExec<TPgDirectFunc<&make_time>, true, true, TArgs_make_time_Policy>({}); }
struct TArgs_makeaclitem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,false,true};
};
TExecFunc arrow_makeaclitem() { return TGenericExec<TPgDirectFunc<&makeaclitem>, true, false, TArgs_makeaclitem_Policy>({}); }
struct TArgs_multirange_gist_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_multirange_gist_compress() { return TGenericExec<TPgDirectFunc<&multirange_gist_compress>, true, true, TArgs_multirange_gist_compress_Policy>({}); }
struct TArgs_multirange_gist_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_multirange_gist_consistent() { return TGenericExec<TPgDirectFunc<&multirange_gist_consistent>, true, true, TArgs_multirange_gist_consistent_Policy>({}); }
struct TArgs_multirange_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_multirange_typanalyze() { return TGenericExec<TPgDirectFunc<&multirange_typanalyze>, true, true, TArgs_multirange_typanalyze_Policy>({}); }
struct TArgs_name_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_name_text() { return TGenericExec<TPgDirectFunc<&name_text>, true, false, TArgs_name_text_Policy>({}); }
struct TArgs_nameeqtext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameeqtext() { return TGenericExec<TPgDirectFunc<&nameeqtext>, true, true, TArgs_nameeqtext_Policy>({}); }
struct TArgs_namegetext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegetext() { return TGenericExec<TPgDirectFunc<&namegetext>, true, true, TArgs_namegetext_Policy>({}); }
struct TArgs_namegttext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegttext() { return TGenericExec<TPgDirectFunc<&namegttext>, true, true, TArgs_namegttext_Policy>({}); }
struct TArgs_nameletext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameletext() { return TGenericExec<TPgDirectFunc<&nameletext>, true, true, TArgs_nameletext_Policy>({}); }
struct TArgs_namelttext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelttext() { return TGenericExec<TPgDirectFunc<&namelttext>, true, true, TArgs_namelttext_Policy>({}); }
struct TArgs_namenetext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namenetext() { return TGenericExec<TPgDirectFunc<&namenetext>, true, true, TArgs_namenetext_Policy>({}); }
struct TArgs_oidtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidtoi8() { return TGenericExec<TPgDirectFunc<&oidtoi8>, true, true, TArgs_oidtoi8_Policy>({}); }
struct TArgs_overlaps_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_overlaps_time() { return TGenericExec<TPgDirectFunc<&overlaps_time>, false, true, TArgs_overlaps_time_Policy>({}); }
struct TArgs_overlaps_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
TExecFunc arrow_overlaps_timetz() { return TGenericExec<TPgDirectFunc<&overlaps_timetz>, false, true, TArgs_overlaps_timetz_Policy>({}); }
struct TArgs_pg_advisory_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_lock_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_lock_int4>, true, true, TArgs_pg_advisory_lock_int4_Policy>({}); }
struct TArgs_pg_advisory_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_lock_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_lock_int8>, true, true, TArgs_pg_advisory_lock_int8_Policy>({}); }
struct TArgs_pg_advisory_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_lock_shared_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_lock_shared_int4>, true, true, TArgs_pg_advisory_lock_shared_int4_Policy>({}); }
struct TArgs_pg_advisory_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_lock_shared_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_lock_shared_int8>, true, true, TArgs_pg_advisory_lock_shared_int8_Policy>({}); }
struct TArgs_pg_advisory_unlock_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_advisory_unlock_all() { return TGenericExec<TPgDirectFunc<&pg_advisory_unlock_all>, true, true, TArgs_pg_advisory_unlock_all_Policy>({}); }
struct TArgs_pg_advisory_unlock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_unlock_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_unlock_int4>, true, true, TArgs_pg_advisory_unlock_int4_Policy>({}); }
struct TArgs_pg_advisory_unlock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_unlock_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_unlock_int8>, true, true, TArgs_pg_advisory_unlock_int8_Policy>({}); }
struct TArgs_pg_advisory_unlock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_unlock_shared_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_unlock_shared_int4>, true, true, TArgs_pg_advisory_unlock_shared_int4_Policy>({}); }
struct TArgs_pg_advisory_unlock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_unlock_shared_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_unlock_shared_int8>, true, true, TArgs_pg_advisory_unlock_shared_int8_Policy>({}); }
struct TArgs_pg_advisory_xact_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_xact_lock_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_xact_lock_int4>, true, true, TArgs_pg_advisory_xact_lock_int4_Policy>({}); }
struct TArgs_pg_advisory_xact_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_xact_lock_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_xact_lock_int8>, true, true, TArgs_pg_advisory_xact_lock_int8_Policy>({}); }
struct TArgs_pg_advisory_xact_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_advisory_xact_lock_shared_int4() { return TGenericExec<TPgDirectFunc<&pg_advisory_xact_lock_shared_int4>, true, true, TArgs_pg_advisory_xact_lock_shared_int4_Policy>({}); }
struct TArgs_pg_advisory_xact_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_advisory_xact_lock_shared_int8() { return TGenericExec<TPgDirectFunc<&pg_advisory_xact_lock_shared_int8>, true, true, TArgs_pg_advisory_xact_lock_shared_int8_Policy>({}); }
struct TArgs_pg_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_blocking_pids() { return TGenericExec<TPgDirectFunc<&pg_blocking_pids>, true, false, TArgs_pg_blocking_pids_Policy>({}); }
struct TArgs_pg_collation_actual_version_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_actual_version() { return TGenericExec<TPgDirectFunc<&pg_collation_actual_version>, true, false, TArgs_pg_collation_actual_version_Policy>({}); }
struct TArgs_pg_column_compression_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_column_compression() { return TGenericExec<TPgDirectFunc<&pg_column_compression>, true, false, TArgs_pg_column_compression_Policy>({}); }
struct TArgs_pg_column_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_column_size() { return TGenericExec<TPgDirectFunc<&pg_column_size>, true, true, TArgs_pg_column_size_Policy>({}); }
struct TArgs_pg_dependencies_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_in() { return TGenericExec<TPgDirectFunc<&pg_dependencies_in>, true, false, TArgs_pg_dependencies_in_Policy>({}); }
struct TArgs_pg_dependencies_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_out() { return TGenericExec<TPgDirectFunc<&pg_dependencies_out>, true, false, TArgs_pg_dependencies_out_Policy>({}); }
struct TArgs_pg_dependencies_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_dependencies_recv() { return TGenericExec<TPgDirectFunc<&pg_dependencies_recv>, true, false, TArgs_pg_dependencies_recv_Policy>({}); }
struct TArgs_pg_dependencies_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_dependencies_send() { return TGenericExec<TPgDirectFunc<&pg_dependencies_send>, true, false, TArgs_pg_dependencies_send_Policy>({}); }
struct TArgs_pg_event_trigger_table_rewrite_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_event_trigger_table_rewrite_oid() { return TGenericExec<TPgDirectFunc<&pg_event_trigger_table_rewrite_oid>, true, true, TArgs_pg_event_trigger_table_rewrite_oid_Policy>({}); }
struct TArgs_pg_event_trigger_table_rewrite_reason_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_event_trigger_table_rewrite_reason() { return TGenericExec<TPgDirectFunc<&pg_event_trigger_table_rewrite_reason>, true, true, TArgs_pg_event_trigger_table_rewrite_reason_Policy>({}); }
struct TArgs_pg_has_role_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_has_role_id() { return TGenericExec<TPgDirectFunc<&pg_has_role_id>, true, true, TArgs_pg_has_role_id_Policy>({}); }
struct TArgs_pg_has_role_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_pg_has_role_id_id() { return TGenericExec<TPgDirectFunc<&pg_has_role_id_id>, true, true, TArgs_pg_has_role_id_id_Policy>({}); }
struct TArgs_pg_has_role_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_has_role_id_name() { return TGenericExec<TPgDirectFunc<&pg_has_role_id_name>, true, true, TArgs_pg_has_role_id_name_Policy>({}); }
struct TArgs_pg_has_role_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_has_role_name() { return TGenericExec<TPgDirectFunc<&pg_has_role_name>, true, true, TArgs_pg_has_role_name_Policy>({}); }
struct TArgs_pg_has_role_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_pg_has_role_name_id() { return TGenericExec<TPgDirectFunc<&pg_has_role_name_id>, true, true, TArgs_pg_has_role_name_id_Policy>({}); }
struct TArgs_pg_has_role_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_pg_has_role_name_name() { return TGenericExec<TPgDirectFunc<&pg_has_role_name_name>, true, true, TArgs_pg_has_role_name_name_Policy>({}); }
struct TArgs_pg_import_system_collations_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_import_system_collations() { return TGenericExec<TPgDirectFunc<&pg_import_system_collations>, true, true, TArgs_pg_import_system_collations_Policy>({}); }
struct TArgs_pg_isolation_test_session_is_blocked_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_isolation_test_session_is_blocked() { return TGenericExec<TPgDirectFunc<&pg_isolation_test_session_is_blocked>, true, true, TArgs_pg_isolation_test_session_is_blocked_Policy>({}); }
struct TArgs_pg_logical_emit_message_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_logical_emit_message_bytea() { return TGenericExec<TPgDirectFunc<&pg_logical_emit_message_bytea>, true, true, TArgs_pg_logical_emit_message_bytea_Policy>({}); }
struct TArgs_pg_logical_emit_message_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_logical_emit_message_text() { return TGenericExec<TPgDirectFunc<&pg_logical_emit_message_text>, true, true, TArgs_pg_logical_emit_message_text_Policy>({}); }
struct TArgs_pg_mcv_list_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_in() { return TGenericExec<TPgDirectFunc<&pg_mcv_list_in>, true, false, TArgs_pg_mcv_list_in_Policy>({}); }
struct TArgs_pg_mcv_list_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_out() { return TGenericExec<TPgDirectFunc<&pg_mcv_list_out>, true, false, TArgs_pg_mcv_list_out_Policy>({}); }
struct TArgs_pg_mcv_list_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_mcv_list_recv() { return TGenericExec<TPgDirectFunc<&pg_mcv_list_recv>, true, false, TArgs_pg_mcv_list_recv_Policy>({}); }
struct TArgs_pg_mcv_list_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_mcv_list_send() { return TGenericExec<TPgDirectFunc<&pg_mcv_list_send>, true, false, TArgs_pg_mcv_list_send_Policy>({}); }
struct TArgs_pg_notification_queue_usage_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_notification_queue_usage() { return TGenericExec<TPgDirectFunc<&pg_notification_queue_usage>, true, true, TArgs_pg_notification_queue_usage_Policy>({}); }
struct TArgs_pg_notify_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_notify() { return TGenericExec<TPgDirectFunc<&pg_notify>, false, true, TArgs_pg_notify_Policy>({}); }
struct TArgs_pg_relation_is_publishable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_is_publishable() { return TGenericExec<TPgDirectFunc<&pg_relation_is_publishable>, true, true, TArgs_pg_relation_is_publishable_Policy>({}); }
struct TArgs_pg_safe_snapshot_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_safe_snapshot_blocking_pids() { return TGenericExec<TPgDirectFunc<&pg_safe_snapshot_blocking_pids>, true, false, TArgs_pg_safe_snapshot_blocking_pids_Policy>({}); }
struct TArgs_pg_stat_get_wal_receiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_wal_receiver() { return TGenericExec<TPgDirectFunc<&pg_stat_get_wal_receiver>, false, false, TArgs_pg_stat_get_wal_receiver_Policy>({}); }
struct TArgs_pg_try_advisory_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_lock_int4() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_lock_int4>, true, true, TArgs_pg_try_advisory_lock_int4_Policy>({}); }
struct TArgs_pg_try_advisory_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_lock_int8() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_lock_int8>, true, true, TArgs_pg_try_advisory_lock_int8_Policy>({}); }
struct TArgs_pg_try_advisory_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_lock_shared_int4() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_lock_shared_int4>, true, true, TArgs_pg_try_advisory_lock_shared_int4_Policy>({}); }
struct TArgs_pg_try_advisory_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_lock_shared_int8() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_lock_shared_int8>, true, true, TArgs_pg_try_advisory_lock_shared_int8_Policy>({}); }
struct TArgs_pg_try_advisory_xact_lock_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_int4() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_int4>, true, true, TArgs_pg_try_advisory_xact_lock_int4_Policy>({}); }
struct TArgs_pg_try_advisory_xact_lock_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_int8() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_int8>, true, true, TArgs_pg_try_advisory_xact_lock_int8_Policy>({}); }
struct TArgs_pg_try_advisory_xact_lock_shared_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_shared_int4() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_shared_int4>, true, true, TArgs_pg_try_advisory_xact_lock_shared_int4_Policy>({}); }
struct TArgs_pg_try_advisory_xact_lock_shared_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_try_advisory_xact_lock_shared_int8() { return TGenericExec<TPgDirectFunc<&pg_try_advisory_xact_lock_shared_int8>, true, true, TArgs_pg_try_advisory_xact_lock_shared_int8_Policy>({}); }
struct TArgs_phraseto_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_phraseto_tsquery() { return TGenericExec<TPgDirectFunc<&phraseto_tsquery>, true, false, TArgs_phraseto_tsquery_Policy>({}); }
struct TArgs_phraseto_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_phraseto_tsquery_byid() { return TGenericExec<TPgDirectFunc<&phraseto_tsquery_byid>, true, false, TArgs_phraseto_tsquery_byid_Policy>({}); }
struct TArgs_plainto_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_plainto_tsquery() { return TGenericExec<TPgDirectFunc<&plainto_tsquery>, true, false, TArgs_plainto_tsquery_Policy>({}); }
struct TArgs_plainto_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_plainto_tsquery_byid() { return TGenericExec<TPgDirectFunc<&plainto_tsquery_byid>, true, false, TArgs_plainto_tsquery_byid_Policy>({}); }
struct TArgs_query_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xml() { return TGenericExec<TPgDirectFunc<&query_to_xml>, true, false, TArgs_query_to_xml_Policy>({}); }
struct TArgs_query_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xml_and_xmlschema() { return TGenericExec<TPgDirectFunc<&query_to_xml_and_xmlschema>, true, false, TArgs_query_to_xml_and_xmlschema_Policy>({}); }
struct TArgs_query_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_query_to_xmlschema() { return TGenericExec<TPgDirectFunc<&query_to_xmlschema>, true, false, TArgs_query_to_xmlschema_Policy>({}); }
struct TArgs_range_gist_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_range_gist_consistent() { return TGenericExec<TPgDirectFunc<&range_gist_consistent>, true, true, TArgs_range_gist_consistent_Policy>({}); }
struct TArgs_range_gist_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_range_gist_penalty() { return TGenericExec<TPgDirectFunc<&range_gist_penalty>, true, true, TArgs_range_gist_penalty_Policy>({}); }
struct TArgs_range_gist_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_range_gist_picksplit() { return TGenericExec<TPgDirectFunc<&range_gist_picksplit>, true, true, TArgs_range_gist_picksplit_Policy>({}); }
struct TArgs_range_gist_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_range_gist_same() { return TGenericExec<TPgDirectFunc<&range_gist_same>, true, true, TArgs_range_gist_same_Policy>({}); }
struct TArgs_range_gist_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_range_gist_union() { return TGenericExec<TPgDirectFunc<&range_gist_union>, true, false, TArgs_range_gist_union_Policy>({}); }
struct TArgs_range_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_range_typanalyze() { return TGenericExec<TPgDirectFunc<&range_typanalyze>, true, true, TArgs_range_typanalyze_Policy>({}); }
struct TArgs_replace_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_replace_text() { return TGenericExec<TPgDirectFunc<&replace_text>, true, false, TArgs_replace_text_Policy>({}); }
struct TArgs_schema_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xml() { return TGenericExec<TPgDirectFunc<&schema_to_xml>, true, false, TArgs_schema_to_xml_Policy>({}); }
struct TArgs_schema_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xml_and_xmlschema() { return TGenericExec<TPgDirectFunc<&schema_to_xml_and_xmlschema>, true, false, TArgs_schema_to_xml_and_xmlschema_Policy>({}); }
struct TArgs_schema_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_schema_to_xmlschema() { return TGenericExec<TPgDirectFunc<&schema_to_xmlschema>, true, false, TArgs_schema_to_xmlschema_Policy>({}); }
struct TArgs_spg_range_quad_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_choose() { return TGenericExec<TPgDirectFunc<&spg_range_quad_choose>, true, true, TArgs_spg_range_quad_choose_Policy>({}); }
struct TArgs_spg_range_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_config() { return TGenericExec<TPgDirectFunc<&spg_range_quad_config>, true, true, TArgs_spg_range_quad_config_Policy>({}); }
struct TArgs_spg_range_quad_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_inner_consistent() { return TGenericExec<TPgDirectFunc<&spg_range_quad_inner_consistent>, true, true, TArgs_spg_range_quad_inner_consistent_Policy>({}); }
struct TArgs_spg_range_quad_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_leaf_consistent() { return TGenericExec<TPgDirectFunc<&spg_range_quad_leaf_consistent>, true, true, TArgs_spg_range_quad_leaf_consistent_Policy>({}); }
struct TArgs_spg_range_quad_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_range_quad_picksplit() { return TGenericExec<TPgDirectFunc<&spg_range_quad_picksplit>, true, true, TArgs_spg_range_quad_picksplit_Policy>({}); }
struct TArgs_split_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_split_part() { return TGenericExec<TPgDirectFunc<&split_part>, true, false, TArgs_split_part_Policy>({}); }
struct TArgs_string_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_string_agg_finalfn() { return TGenericExec<TPgDirectFunc<&string_agg_finalfn>, false, false, TArgs_string_agg_finalfn_Policy>({}); }
struct TArgs_string_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_string_agg_transfn() { return TGenericExec<TPgDirectFunc<&string_agg_transfn>, false, true, TArgs_string_agg_transfn_Policy>({}); }
struct TArgs_table_to_xml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xml() { return TGenericExec<TPgDirectFunc<&table_to_xml>, true, false, TArgs_table_to_xml_Policy>({}); }
struct TArgs_table_to_xml_and_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xml_and_xmlschema() { return TGenericExec<TPgDirectFunc<&table_to_xml_and_xmlschema>, true, false, TArgs_table_to_xml_and_xmlschema_Policy>({}); }
struct TArgs_table_to_xmlschema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_table_to_xmlschema() { return TGenericExec<TPgDirectFunc<&table_to_xmlschema>, true, false, TArgs_table_to_xmlschema_Policy>({}); }
struct TArgs_text_format_nv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_format_nv() { return TGenericExec<TPgDirectFunc<&text_format_nv>, false, false, TArgs_text_format_nv_Policy>({}); }
struct TArgs_text_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_ge() { return TGenericExec<TPgDirectFunc<&text_ge>, true, true, TArgs_text_ge_Policy>({}); }
struct TArgs_text_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_gt() { return TGenericExec<TPgDirectFunc<&text_gt>, true, true, TArgs_text_gt_Policy>({}); }
struct TArgs_text_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_larger() { return TGenericExec<TPgDirectFunc<&text_larger>, true, false, TArgs_text_larger_Policy>({}); }
struct TArgs_text_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_le() { return TGenericExec<TPgDirectFunc<&text_le>, true, true, TArgs_text_le_Policy>({}); }
struct TArgs_text_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_left() { return TGenericExec<TPgDirectFunc<&text_left>, true, false, TArgs_text_left_Policy>({}); }
struct TArgs_text_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_lt() { return TGenericExec<TPgDirectFunc<&text_lt>, true, true, TArgs_text_lt_Policy>({}); }
struct TArgs_text_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_name() { return TGenericExec<TPgDirectFunc<&text_name>, true, false, TArgs_text_name_Policy>({}); }
struct TArgs_text_pattern_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_ge() { return TGenericExec<TPgDirectFunc<&text_pattern_ge>, true, true, TArgs_text_pattern_ge_Policy>({}); }
struct TArgs_text_pattern_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_gt() { return TGenericExec<TPgDirectFunc<&text_pattern_gt>, true, true, TArgs_text_pattern_gt_Policy>({}); }
struct TArgs_text_pattern_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_le() { return TGenericExec<TPgDirectFunc<&text_pattern_le>, true, true, TArgs_text_pattern_le_Policy>({}); }
struct TArgs_text_pattern_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_pattern_lt() { return TGenericExec<TPgDirectFunc<&text_pattern_lt>, true, true, TArgs_text_pattern_lt_Policy>({}); }
struct TArgs_text_reverse_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_reverse() { return TGenericExec<TPgDirectFunc<&text_reverse>, true, false, TArgs_text_reverse_Policy>({}); }
struct TArgs_text_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_right() { return TGenericExec<TPgDirectFunc<&text_right>, true, false, TArgs_text_right_Policy>({}); }
struct TArgs_text_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_smaller() { return TGenericExec<TPgDirectFunc<&text_smaller>, true, false, TArgs_text_smaller_Policy>({}); }
struct TArgs_text_starts_with_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_starts_with() { return TGenericExec<TPgDirectFunc<&text_starts_with>, true, true, TArgs_text_starts_with_Policy>({}); }
struct TArgs_text_substr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_text_substr() { return TGenericExec<TPgDirectFunc<&text_substr>, true, false, TArgs_text_substr_Policy>({}); }
struct TArgs_text_substr_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_text_substr_no_len() { return TGenericExec<TPgDirectFunc<&text_substr_no_len>, true, false, TArgs_text_substr_no_len_Policy>({}); }
struct TArgs_text_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_text_to_array() { return TGenericExec<TPgDirectFunc<&text_to_array>, false, false, TArgs_text_to_array_Policy>({}); }
struct TArgs_text_to_array_null_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_text_to_array_null() { return TGenericExec<TPgDirectFunc<&text_to_array_null>, false, false, TArgs_text_to_array_null_Policy>({}); }
struct TArgs_textcat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textcat() { return TGenericExec<TPgDirectFunc<&textcat>, true, false, TArgs_textcat_Policy>({}); }
struct TArgs_texteq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texteq() { return TGenericExec<TPgDirectFunc<&texteq>, true, true, TArgs_texteq_Policy>({}); }
struct TArgs_texteqname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texteqname() { return TGenericExec<TPgDirectFunc<&texteqname>, true, true, TArgs_texteqname_Policy>({}); }
struct TArgs_textgename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textgename() { return TGenericExec<TPgDirectFunc<&textgename>, true, true, TArgs_textgename_Policy>({}); }
struct TArgs_textgtname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textgtname() { return TGenericExec<TPgDirectFunc<&textgtname>, true, true, TArgs_textgtname_Policy>({}); }
struct TArgs_textin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textin() { return TGenericExec<TPgDirectFunc<&textin>, true, false, TArgs_textin_Policy>({}); }
struct TArgs_textlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textlen() { return TGenericExec<TPgDirectFunc<&textlen>, true, true, TArgs_textlen_Policy>({}); }
struct TArgs_textlename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textlename() { return TGenericExec<TPgDirectFunc<&textlename>, true, true, TArgs_textlename_Policy>({}); }
struct TArgs_textltname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textltname() { return TGenericExec<TPgDirectFunc<&textltname>, true, true, TArgs_textltname_Policy>({}); }
struct TArgs_textne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textne() { return TGenericExec<TPgDirectFunc<&textne>, true, true, TArgs_textne_Policy>({}); }
struct TArgs_textnename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textnename() { return TGenericExec<TPgDirectFunc<&textnename>, true, true, TArgs_textnename_Policy>({}); }
struct TArgs_textoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textoctetlen() { return TGenericExec<TPgDirectFunc<&textoctetlen>, true, true, TArgs_textoctetlen_Policy>({}); }
struct TArgs_textout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textout() { return TGenericExec<TPgDirectFunc<&textout>, true, false, TArgs_textout_Policy>({}); }
struct TArgs_textoverlay_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_textoverlay() { return TGenericExec<TPgDirectFunc<&textoverlay>, true, false, TArgs_textoverlay_Policy>({}); }
struct TArgs_textoverlay_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_textoverlay_no_len() { return TGenericExec<TPgDirectFunc<&textoverlay_no_len>, true, false, TArgs_textoverlay_no_len_Policy>({}); }
struct TArgs_textpos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textpos() { return TGenericExec<TPgDirectFunc<&textpos>, true, true, TArgs_textpos_Policy>({}); }
struct TArgs_textrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textrecv() { return TGenericExec<TPgDirectFunc<&textrecv>, true, false, TArgs_textrecv_Policy>({}); }
struct TArgs_textsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textsend() { return TGenericExec<TPgDirectFunc<&textsend>, true, false, TArgs_textsend_Policy>({}); }
struct TArgs_texttoxml_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_texttoxml() { return TGenericExec<TPgDirectFunc<&texttoxml>, true, false, TArgs_texttoxml_Policy>({}); }
struct TArgs_time_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_cmp() { return TGenericExec<TPgDirectFunc<&time_cmp>, true, true, TArgs_time_cmp_Policy>({}); }
struct TArgs_time_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_eq() { return TGenericExec<TPgDirectFunc<&time_eq>, true, true, TArgs_time_eq_Policy>({}); }
struct TArgs_time_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_ge() { return TGenericExec<TPgDirectFunc<&time_ge>, true, true, TArgs_time_ge_Policy>({}); }
struct TArgs_time_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_gt() { return TGenericExec<TPgDirectFunc<&time_gt>, true, true, TArgs_time_gt_Policy>({}); }
struct TArgs_time_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_hash() { return TGenericExec<TPgDirectFunc<&time_hash>, true, true, TArgs_time_hash_Policy>({}); }
struct TArgs_time_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_hash_extended() { return TGenericExec<TPgDirectFunc<&time_hash_extended>, true, true, TArgs_time_hash_extended_Policy>({}); }
struct TArgs_time_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_time_in() { return TGenericExec<TPgDirectFunc<&time_in>, true, true, TArgs_time_in_Policy>({}); }
struct TArgs_time_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_interval() { return TGenericExec<TPgDirectFunc<&time_interval>, true, false, TArgs_time_interval_Policy>({}); }
struct TArgs_time_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_larger() { return TGenericExec<TPgDirectFunc<&time_larger>, true, true, TArgs_time_larger_Policy>({}); }
struct TArgs_time_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_le() { return TGenericExec<TPgDirectFunc<&time_le>, true, true, TArgs_time_le_Policy>({}); }
struct TArgs_time_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_lt() { return TGenericExec<TPgDirectFunc<&time_lt>, true, true, TArgs_time_lt_Policy>({}); }
struct TArgs_time_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_time_mi_interval() { return TGenericExec<TPgDirectFunc<&time_mi_interval>, true, true, TArgs_time_mi_interval_Policy>({}); }
struct TArgs_time_mi_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_mi_time() { return TGenericExec<TPgDirectFunc<&time_mi_time>, true, false, TArgs_time_mi_time_Policy>({}); }
struct TArgs_time_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_ne() { return TGenericExec<TPgDirectFunc<&time_ne>, true, true, TArgs_time_ne_Policy>({}); }
struct TArgs_time_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_out() { return TGenericExec<TPgDirectFunc<&time_out>, true, false, TArgs_time_out_Policy>({}); }
struct TArgs_time_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_time_part() { return TGenericExec<TPgDirectFunc<&time_part>, true, true, TArgs_time_part_Policy>({}); }
struct TArgs_time_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_time_pl_interval() { return TGenericExec<TPgDirectFunc<&time_pl_interval>, true, true, TArgs_time_pl_interval_Policy>({}); }
struct TArgs_time_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_time_recv() { return TGenericExec<TPgDirectFunc<&time_recv>, true, true, TArgs_time_recv_Policy>({}); }
struct TArgs_time_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_scale() { return TGenericExec<TPgDirectFunc<&time_scale>, true, true, TArgs_time_scale_Policy>({}); }
struct TArgs_time_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_send() { return TGenericExec<TPgDirectFunc<&time_send>, true, false, TArgs_time_send_Policy>({}); }
struct TArgs_time_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_time_smaller() { return TGenericExec<TPgDirectFunc<&time_smaller>, true, true, TArgs_time_smaller_Policy>({}); }
struct TArgs_time_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_support() { return TGenericExec<TPgDirectFunc<&time_support>, true, true, TArgs_time_support_Policy>({}); }
struct TArgs_time_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_time_timetz() { return TGenericExec<TPgDirectFunc<&time_timetz>, true, false, TArgs_time_timetz_Policy>({}); }
struct TArgs_timestamp_cmp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_cmp_date() { return TGenericExec<TPgDirectFunc<&timestamp_cmp_date>, true, true, TArgs_timestamp_cmp_date_Policy>({}); }
struct TArgs_timestamp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_date() { return TGenericExec<TPgDirectFunc<&timestamp_date>, true, true, TArgs_timestamp_date_Policy>({}); }
struct TArgs_timestamp_eq_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_eq_date() { return TGenericExec<TPgDirectFunc<&timestamp_eq_date>, true, true, TArgs_timestamp_eq_date_Policy>({}); }
struct TArgs_timestamp_ge_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ge_date() { return TGenericExec<TPgDirectFunc<&timestamp_ge_date>, true, true, TArgs_timestamp_ge_date_Policy>({}); }
struct TArgs_timestamp_gt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_gt_date() { return TGenericExec<TPgDirectFunc<&timestamp_gt_date>, true, true, TArgs_timestamp_gt_date_Policy>({}); }
struct TArgs_timestamp_le_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_le_date() { return TGenericExec<TPgDirectFunc<&timestamp_le_date>, true, true, TArgs_timestamp_le_date_Policy>({}); }
struct TArgs_timestamp_lt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_lt_date() { return TGenericExec<TPgDirectFunc<&timestamp_lt_date>, true, true, TArgs_timestamp_lt_date_Policy>({}); }
struct TArgs_timestamp_ne_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ne_date() { return TGenericExec<TPgDirectFunc<&timestamp_ne_date>, true, true, TArgs_timestamp_ne_date_Policy>({}); }
struct TArgs_timestamp_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_time() { return TGenericExec<TPgDirectFunc<&timestamp_time>, true, true, TArgs_timestamp_time_Policy>({}); }
struct TArgs_timestamptz_cmp_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_cmp_date() { return TGenericExec<TPgDirectFunc<&timestamptz_cmp_date>, true, true, TArgs_timestamptz_cmp_date_Policy>({}); }
struct TArgs_timestamptz_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_date() { return TGenericExec<TPgDirectFunc<&timestamptz_date>, true, true, TArgs_timestamptz_date_Policy>({}); }
struct TArgs_timestamptz_eq_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_eq_date() { return TGenericExec<TPgDirectFunc<&timestamptz_eq_date>, true, true, TArgs_timestamptz_eq_date_Policy>({}); }
struct TArgs_timestamptz_ge_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ge_date() { return TGenericExec<TPgDirectFunc<&timestamptz_ge_date>, true, true, TArgs_timestamptz_ge_date_Policy>({}); }
struct TArgs_timestamptz_gt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_gt_date() { return TGenericExec<TPgDirectFunc<&timestamptz_gt_date>, true, true, TArgs_timestamptz_gt_date_Policy>({}); }
struct TArgs_timestamptz_le_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_le_date() { return TGenericExec<TPgDirectFunc<&timestamptz_le_date>, true, true, TArgs_timestamptz_le_date_Policy>({}); }
struct TArgs_timestamptz_lt_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_lt_date() { return TGenericExec<TPgDirectFunc<&timestamptz_lt_date>, true, true, TArgs_timestamptz_lt_date_Policy>({}); }
struct TArgs_timestamptz_ne_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ne_date() { return TGenericExec<TPgDirectFunc<&timestamptz_ne_date>, true, true, TArgs_timestamptz_ne_date_Policy>({}); }
struct TArgs_timestamptz_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_time() { return TGenericExec<TPgDirectFunc<&timestamptz_time>, true, true, TArgs_timestamptz_time_Policy>({}); }
struct TArgs_timestamptz_timetz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_timetz() { return TGenericExec<TPgDirectFunc<&timestamptz_timetz>, true, false, TArgs_timestamptz_timetz_Policy>({}); }
struct TArgs_timetypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetypmodin() { return TGenericExec<TPgDirectFunc<&timetypmodin>, true, true, TArgs_timetypmodin_Policy>({}); }
struct TArgs_timetypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timetypmodout() { return TGenericExec<TPgDirectFunc<&timetypmodout>, true, false, TArgs_timetypmodout_Policy>({}); }
struct TArgs_timetz_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_cmp() { return TGenericExec<TPgDirectFunc<&timetz_cmp>, true, true, TArgs_timetz_cmp_Policy>({}); }
struct TArgs_timetz_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_eq() { return TGenericExec<TPgDirectFunc<&timetz_eq>, true, true, TArgs_timetz_eq_Policy>({}); }
struct TArgs_timetz_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_ge() { return TGenericExec<TPgDirectFunc<&timetz_ge>, true, true, TArgs_timetz_ge_Policy>({}); }
struct TArgs_timetz_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_gt() { return TGenericExec<TPgDirectFunc<&timetz_gt>, true, true, TArgs_timetz_gt_Policy>({}); }
struct TArgs_timetz_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_hash() { return TGenericExec<TPgDirectFunc<&timetz_hash>, true, true, TArgs_timetz_hash_Policy>({}); }
struct TArgs_timetz_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timetz_hash_extended() { return TGenericExec<TPgDirectFunc<&timetz_hash_extended>, true, true, TArgs_timetz_hash_extended_Policy>({}); }
struct TArgs_timetz_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timetz_in() { return TGenericExec<TPgDirectFunc<&timetz_in>, true, false, TArgs_timetz_in_Policy>({}); }
struct TArgs_timetz_izone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_izone() { return TGenericExec<TPgDirectFunc<&timetz_izone>, true, false, TArgs_timetz_izone_Policy>({}); }
struct TArgs_timetz_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_larger() { return TGenericExec<TPgDirectFunc<&timetz_larger>, true, false, TArgs_timetz_larger_Policy>({}); }
struct TArgs_timetz_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_le() { return TGenericExec<TPgDirectFunc<&timetz_le>, true, true, TArgs_timetz_le_Policy>({}); }
struct TArgs_timetz_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_lt() { return TGenericExec<TPgDirectFunc<&timetz_lt>, true, true, TArgs_timetz_lt_Policy>({}); }
struct TArgs_timetz_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_mi_interval() { return TGenericExec<TPgDirectFunc<&timetz_mi_interval>, true, false, TArgs_timetz_mi_interval_Policy>({}); }
struct TArgs_timetz_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_ne() { return TGenericExec<TPgDirectFunc<&timetz_ne>, true, true, TArgs_timetz_ne_Policy>({}); }
struct TArgs_timetz_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_out() { return TGenericExec<TPgDirectFunc<&timetz_out>, true, false, TArgs_timetz_out_Policy>({}); }
struct TArgs_timetz_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_part() { return TGenericExec<TPgDirectFunc<&timetz_part>, true, true, TArgs_timetz_part_Policy>({}); }
struct TArgs_timetz_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_pl_interval() { return TGenericExec<TPgDirectFunc<&timetz_pl_interval>, true, false, TArgs_timetz_pl_interval_Policy>({}); }
struct TArgs_timetz_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_timetz_recv() { return TGenericExec<TPgDirectFunc<&timetz_recv>, true, false, TArgs_timetz_recv_Policy>({}); }
struct TArgs_timetz_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timetz_scale() { return TGenericExec<TPgDirectFunc<&timetz_scale>, true, false, TArgs_timetz_scale_Policy>({}); }
struct TArgs_timetz_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_send() { return TGenericExec<TPgDirectFunc<&timetz_send>, true, false, TArgs_timetz_send_Policy>({}); }
struct TArgs_timetz_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_smaller() { return TGenericExec<TPgDirectFunc<&timetz_smaller>, true, false, TArgs_timetz_smaller_Policy>({}); }
struct TArgs_timetz_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetz_time() { return TGenericExec<TPgDirectFunc<&timetz_time>, true, true, TArgs_timetz_time_Policy>({}); }
struct TArgs_timetz_zone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_timetz_zone() { return TGenericExec<TPgDirectFunc<&timetz_zone>, true, false, TArgs_timetz_zone_Policy>({}); }
struct TArgs_timetztypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timetztypmodin() { return TGenericExec<TPgDirectFunc<&timetztypmodin>, true, true, TArgs_timetztypmodin_Policy>({}); }
struct TArgs_timetztypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timetztypmodout() { return TGenericExec<TPgDirectFunc<&timetztypmodout>, true, false, TArgs_timetztypmodout_Policy>({}); }
struct TArgs_to_hex32_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_hex32() { return TGenericExec<TPgDirectFunc<&to_hex32>, true, false, TArgs_to_hex32_Policy>({}); }
struct TArgs_to_hex64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_hex64() { return TGenericExec<TPgDirectFunc<&to_hex64>, true, false, TArgs_to_hex64_Policy>({}); }
struct TArgs_to_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_tsquery() { return TGenericExec<TPgDirectFunc<&to_tsquery>, true, false, TArgs_to_tsquery_Policy>({}); }
struct TArgs_to_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_to_tsquery_byid() { return TGenericExec<TPgDirectFunc<&to_tsquery_byid>, true, false, TArgs_to_tsquery_byid_Policy>({}); }
struct TArgs_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_tsvector() { return TGenericExec<TPgDirectFunc<&to_tsvector>, true, false, TArgs_to_tsvector_Policy>({}); }
struct TArgs_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_to_tsvector_byid() { return TGenericExec<TPgDirectFunc<&to_tsvector_byid>, true, false, TArgs_to_tsvector_byid_Policy>({}); }
struct TArgs_trim_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_trim_array() { return TGenericExec<TPgDirectFunc<&trim_array>, true, false, TArgs_trim_array_Policy>({}); }
struct TArgs_ts_headline_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline() { return TGenericExec<TPgDirectFunc<&ts_headline>, true, false, TArgs_ts_headline_Policy>({}); }
struct TArgs_ts_headline_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_byid() { return TGenericExec<TPgDirectFunc<&ts_headline_byid>, true, false, TArgs_ts_headline_byid_Policy>({}); }
struct TArgs_ts_headline_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_byid_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_byid_opt>, true, false, TArgs_ts_headline_byid_opt_Policy>({}); }
struct TArgs_ts_headline_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline_json() { return TGenericExec<TPgDirectFunc<&ts_headline_json>, true, false, TArgs_ts_headline_json_Policy>({}); }
struct TArgs_ts_headline_json_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_json_byid() { return TGenericExec<TPgDirectFunc<&ts_headline_json_byid>, true, false, TArgs_ts_headline_json_byid_Policy>({}); }
struct TArgs_ts_headline_json_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_json_byid_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_json_byid_opt>, true, false, TArgs_ts_headline_json_byid_opt_Policy>({}); }
struct TArgs_ts_headline_json_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_json_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_json_opt>, true, false, TArgs_ts_headline_json_opt_Policy>({}); }
struct TArgs_ts_headline_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_headline_jsonb() { return TGenericExec<TPgDirectFunc<&ts_headline_jsonb>, true, false, TArgs_ts_headline_jsonb_Policy>({}); }
struct TArgs_ts_headline_jsonb_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_ts_headline_jsonb_byid() { return TGenericExec<TPgDirectFunc<&ts_headline_jsonb_byid>, true, false, TArgs_ts_headline_jsonb_byid_Policy>({}); }
struct TArgs_ts_headline_jsonb_byid_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_ts_headline_jsonb_byid_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_jsonb_byid_opt>, true, false, TArgs_ts_headline_jsonb_byid_opt_Policy>({}); }
struct TArgs_ts_headline_jsonb_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_jsonb_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_jsonb_opt>, true, false, TArgs_ts_headline_jsonb_opt_Policy>({}); }
struct TArgs_ts_headline_opt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_headline_opt() { return TGenericExec<TPgDirectFunc<&ts_headline_opt>, true, false, TArgs_ts_headline_opt_Policy>({}); }
struct TArgs_ts_match_qv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_qv() { return TGenericExec<TPgDirectFunc<&ts_match_qv>, true, true, TArgs_ts_match_qv_Policy>({}); }
struct TArgs_ts_match_tq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_tq() { return TGenericExec<TPgDirectFunc<&ts_match_tq>, true, true, TArgs_ts_match_tq_Policy>({}); }
struct TArgs_ts_match_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_tt() { return TGenericExec<TPgDirectFunc<&ts_match_tt>, true, true, TArgs_ts_match_tt_Policy>({}); }
struct TArgs_ts_match_vq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_vq() { return TGenericExec<TPgDirectFunc<&ts_match_vq>, true, true, TArgs_ts_match_vq_Policy>({}); }
struct TArgs_ts_rank_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_rank_tt() { return TGenericExec<TPgDirectFunc<&ts_rank_tt>, true, true, TArgs_ts_rank_tt_Policy>({}); }
struct TArgs_ts_rank_ttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_ts_rank_ttf() { return TGenericExec<TPgDirectFunc<&ts_rank_ttf>, true, true, TArgs_ts_rank_ttf_Policy>({}); }
struct TArgs_ts_rank_wtt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_rank_wtt() { return TGenericExec<TPgDirectFunc<&ts_rank_wtt>, true, true, TArgs_ts_rank_wtt_Policy>({}); }
struct TArgs_ts_rank_wttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_ts_rank_wttf() { return TGenericExec<TPgDirectFunc<&ts_rank_wttf>, true, true, TArgs_ts_rank_wttf_Policy>({}); }
struct TArgs_ts_rankcd_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_rankcd_tt() { return TGenericExec<TPgDirectFunc<&ts_rankcd_tt>, true, true, TArgs_ts_rankcd_tt_Policy>({}); }
struct TArgs_ts_rankcd_ttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_ts_rankcd_ttf() { return TGenericExec<TPgDirectFunc<&ts_rankcd_ttf>, true, true, TArgs_ts_rankcd_ttf_Policy>({}); }
struct TArgs_ts_rankcd_wtt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_ts_rankcd_wtt() { return TGenericExec<TPgDirectFunc<&ts_rankcd_wtt>, true, true, TArgs_ts_rankcd_wtt_Policy>({}); }
struct TArgs_ts_rankcd_wttf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_ts_rankcd_wttf() { return TGenericExec<TPgDirectFunc<&ts_rankcd_wttf>, true, true, TArgs_ts_rankcd_wttf_Policy>({}); }
struct TArgs_tsvector_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_cmp() { return TGenericExec<TPgDirectFunc<&tsvector_cmp>, true, true, TArgs_tsvector_cmp_Policy>({}); }
struct TArgs_tsvector_concat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_concat() { return TGenericExec<TPgDirectFunc<&tsvector_concat>, true, false, TArgs_tsvector_concat_Policy>({}); }
struct TArgs_tsvector_delete_arr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_delete_arr() { return TGenericExec<TPgDirectFunc<&tsvector_delete_arr>, true, false, TArgs_tsvector_delete_arr_Policy>({}); }
struct TArgs_tsvector_delete_str_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_delete_str() { return TGenericExec<TPgDirectFunc<&tsvector_delete_str>, true, false, TArgs_tsvector_delete_str_Policy>({}); }
struct TArgs_tsvector_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_eq() { return TGenericExec<TPgDirectFunc<&tsvector_eq>, true, true, TArgs_tsvector_eq_Policy>({}); }
struct TArgs_tsvector_filter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_filter() { return TGenericExec<TPgDirectFunc<&tsvector_filter>, true, false, TArgs_tsvector_filter_Policy>({}); }
struct TArgs_tsvector_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_ge() { return TGenericExec<TPgDirectFunc<&tsvector_ge>, true, true, TArgs_tsvector_ge_Policy>({}); }
struct TArgs_tsvector_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_gt() { return TGenericExec<TPgDirectFunc<&tsvector_gt>, true, true, TArgs_tsvector_gt_Policy>({}); }
struct TArgs_tsvector_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_le() { return TGenericExec<TPgDirectFunc<&tsvector_le>, true, true, TArgs_tsvector_le_Policy>({}); }
struct TArgs_tsvector_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_length() { return TGenericExec<TPgDirectFunc<&tsvector_length>, true, true, TArgs_tsvector_length_Policy>({}); }
struct TArgs_tsvector_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_lt() { return TGenericExec<TPgDirectFunc<&tsvector_lt>, true, true, TArgs_tsvector_lt_Policy>({}); }
struct TArgs_tsvector_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_ne() { return TGenericExec<TPgDirectFunc<&tsvector_ne>, true, true, TArgs_tsvector_ne_Policy>({}); }
struct TArgs_tsvector_setweight_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_tsvector_setweight() { return TGenericExec<TPgDirectFunc<&tsvector_setweight>, true, false, TArgs_tsvector_setweight_Policy>({}); }
struct TArgs_tsvector_setweight_by_filter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_tsvector_setweight_by_filter() { return TGenericExec<TPgDirectFunc<&tsvector_setweight_by_filter>, true, false, TArgs_tsvector_setweight_by_filter_Policy>({}); }
struct TArgs_tsvector_strip_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_strip() { return TGenericExec<TPgDirectFunc<&tsvector_strip>, true, false, TArgs_tsvector_strip_Policy>({}); }
struct TArgs_tsvector_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_to_array() { return TGenericExec<TPgDirectFunc<&tsvector_to_array>, true, false, TArgs_tsvector_to_array_Policy>({}); }
struct TArgs_tsvector_update_trigger_bycolumn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_tsvector_update_trigger_bycolumn() { return TGenericExec<TPgDirectFunc<&tsvector_update_trigger_bycolumn>, false, true, TArgs_tsvector_update_trigger_bycolumn_Policy>({}); }
struct TArgs_tsvector_update_trigger_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_tsvector_update_trigger_byid() { return TGenericExec<TPgDirectFunc<&tsvector_update_trigger_byid>, false, true, TArgs_tsvector_update_trigger_byid_Policy>({}); }
struct TArgs_unicode_is_normalized_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_unicode_is_normalized() { return TGenericExec<TPgDirectFunc<&unicode_is_normalized>, true, true, TArgs_unicode_is_normalized_Policy>({}); }
struct TArgs_unicode_normalize_func_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_unicode_normalize_func() { return TGenericExec<TPgDirectFunc<&unicode_normalize_func>, true, false, TArgs_unicode_normalize_func_Policy>({}); }
struct TArgs_unique_key_recheck_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_unique_key_recheck() { return TGenericExec<TPgDirectFunc<&unique_key_recheck>, true, true, TArgs_unique_key_recheck_Policy>({}); }
struct TArgs_unistr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unistr() { return TGenericExec<TPgDirectFunc<&unistr>, true, false, TArgs_unistr_Policy>({}); }
struct TArgs_unknownin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownin() { return TGenericExec<TPgDirectFunc<&unknownin>, true, false, TArgs_unknownin_Policy>({}); }
struct TArgs_unknownout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownout() { return TGenericExec<TPgDirectFunc<&unknownout>, true, false, TArgs_unknownout_Policy>({}); }
struct TArgs_unknownrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_unknownrecv() { return TGenericExec<TPgDirectFunc<&unknownrecv>, true, false, TArgs_unknownrecv_Policy>({}); }
struct TArgs_unknownsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_unknownsend() { return TGenericExec<TPgDirectFunc<&unknownsend>, true, false, TArgs_unknownsend_Policy>({}); }
struct TArgs_websearch_to_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_websearch_to_tsquery() { return TGenericExec<TPgDirectFunc<&websearch_to_tsquery>, true, false, TArgs_websearch_to_tsquery_Policy>({}); }
struct TArgs_websearch_to_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_websearch_to_tsquery_byid() { return TGenericExec<TPgDirectFunc<&websearch_to_tsquery_byid>, true, false, TArgs_websearch_to_tsquery_byid_Policy>({}); }
struct TArgs_width_bucket_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_width_bucket_array() { return TGenericExec<TPgDirectFunc<&width_bucket_array>, true, true, TArgs_width_bucket_array_Policy>({}); }
struct TArgs_xml_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_in() { return TGenericExec<TPgDirectFunc<&xml_in>, true, false, TArgs_xml_in_Policy>({}); }
struct TArgs_xml_is_well_formed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed() { return TGenericExec<TPgDirectFunc<&xml_is_well_formed>, true, true, TArgs_xml_is_well_formed_Policy>({}); }
struct TArgs_xml_is_well_formed_content_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed_content() { return TGenericExec<TPgDirectFunc<&xml_is_well_formed_content>, true, true, TArgs_xml_is_well_formed_content_Policy>({}); }
struct TArgs_xml_is_well_formed_document_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_is_well_formed_document() { return TGenericExec<TPgDirectFunc<&xml_is_well_formed_document>, true, true, TArgs_xml_is_well_formed_document_Policy>({}); }
struct TArgs_xml_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_out() { return TGenericExec<TPgDirectFunc<&xml_out>, true, false, TArgs_xml_out_Policy>({}); }
struct TArgs_xml_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xml_recv() { return TGenericExec<TPgDirectFunc<&xml_recv>, true, false, TArgs_xml_recv_Policy>({}); }
struct TArgs_xml_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xml_send() { return TGenericExec<TPgDirectFunc<&xml_send>, true, false, TArgs_xml_send_Policy>({}); }
struct TArgs_xmlcomment_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xmlcomment() { return TGenericExec<TPgDirectFunc<&xmlcomment>, true, false, TArgs_xmlcomment_Policy>({}); }
struct TArgs_xmlconcat2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlconcat2() { return TGenericExec<TPgDirectFunc<&xmlconcat2>, false, false, TArgs_xmlconcat2_Policy>({}); }
struct TArgs_xmlexists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlexists() { return TGenericExec<TPgDirectFunc<&xmlexists>, true, true, TArgs_xmlexists_Policy>({}); }
struct TArgs_xmltotext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xmltotext() { return TGenericExec<TPgDirectFunc<&xmltotext>, true, false, TArgs_xmltotext_Policy>({}); }
struct TArgs_xmlvalidate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_xmlvalidate() { return TGenericExec<TPgDirectFunc<&xmlvalidate>, true, true, TArgs_xmlvalidate_Policy>({}); }
struct TArgs_xpath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_xpath() { return TGenericExec<TPgDirectFunc<&xpath>, true, false, TArgs_xpath_Policy>({}); }
struct TArgs_xpath_exists_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_xpath_exists() { return TGenericExec<TPgDirectFunc<&xpath_exists>, true, true, TArgs_xpath_exists_Policy>({}); }
