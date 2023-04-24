struct TArgs_areajoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_areajoinsel() { return &GenericExec<TPgDirectFunc<&areajoinsel>, true, true, TArgs_areajoinsel_Policy>; }
struct TArgs_areasel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_areasel() { return &GenericExec<TPgDirectFunc<&areasel>, true, true, TArgs_areasel_Policy>; }
struct TArgs_array_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_typanalyze() { return &GenericExec<TPgDirectFunc<&array_typanalyze>, true, true, TArgs_array_typanalyze_Policy>; }
struct TArgs_arraycontjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_arraycontjoinsel() { return &GenericExec<TPgDirectFunc<&arraycontjoinsel>, true, true, TArgs_arraycontjoinsel_Policy>; }
struct TArgs_arraycontsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_arraycontsel() { return &GenericExec<TPgDirectFunc<&arraycontsel>, true, true, TArgs_arraycontsel_Policy>; }
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
struct TArgs_btnamecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btnamecmp() { return &GenericExec<TPgDirectFunc<&btnamecmp>, true, true, TArgs_btnamecmp_Policy>; }
struct TArgs_btnamesortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btnamesortsupport() { return &GenericExec<TPgDirectFunc<&btnamesortsupport>, true, true, TArgs_btnamesortsupport_Policy>; }
struct TArgs_btrecordcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btrecordcmp() { return &GenericExec<TPgDirectFunc<&btrecordcmp>, true, true, TArgs_btrecordcmp_Policy>; }
struct TArgs_btrecordimagecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btrecordimagecmp() { return &GenericExec<TPgDirectFunc<&btrecordimagecmp>, true, true, TArgs_btrecordimagecmp_Policy>; }
struct TArgs_bttidcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bttidcmp() { return &GenericExec<TPgDirectFunc<&bttidcmp>, true, true, TArgs_bttidcmp_Policy>; }
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
struct TArgs_contjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_contjoinsel() { return &GenericExec<TPgDirectFunc<&contjoinsel>, true, true, TArgs_contjoinsel_Policy>; }
struct TArgs_contsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_contsel() { return &GenericExec<TPgDirectFunc<&contsel>, true, true, TArgs_contsel_Policy>; }
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
struct TArgs_current_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_schema() { return &GenericExec<TPgDirectFunc<&current_schema>, true, false, TArgs_current_schema_Policy>; }
struct TArgs_current_schemas_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_current_schemas() { return &GenericExec<TPgDirectFunc<&current_schemas>, true, false, TArgs_current_schemas_Policy>; }
struct TArgs_current_user_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_user() { return &GenericExec<TPgDirectFunc<&current_user>, true, false, TArgs_current_user_Policy>; }
struct TArgs_currtid_byrelname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_currtid_byrelname() { return &GenericExec<TPgDirectFunc<&currtid_byrelname>, true, false, TArgs_currtid_byrelname_Policy>; }
struct TArgs_currval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_currval_oid() { return &GenericExec<TPgDirectFunc<&currval_oid>, true, true, TArgs_currval_oid_Policy>; }
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
struct TArgs_dtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi8() { return &GenericExec<TPgDirectFunc<&dtoi8>, true, true, TArgs_dtoi8_Policy>; }
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
struct TArgs_ftoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi8() { return &GenericExec<TPgDirectFunc<&ftoi8>, true, true, TArgs_ftoi8_Policy>; }
struct TArgs_generate_series_int8_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int8_support() { return &GenericExec<TPgDirectFunc<&generate_series_int8_support>, true, true, TArgs_generate_series_int8_support_Policy>; }
struct TArgs_gin_clean_pending_list_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gin_clean_pending_list() { return &GenericExec<TPgDirectFunc<&gin_clean_pending_list>, true, true, TArgs_gin_clean_pending_list_Policy>; }
struct TArgs_gin_compare_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_gin_compare_jsonb() { return &GenericExec<TPgDirectFunc<&gin_compare_jsonb>, true, true, TArgs_gin_compare_jsonb_Policy>; }
struct TArgs_gin_consistent_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_consistent_jsonb() { return &GenericExec<TPgDirectFunc<&gin_consistent_jsonb>, true, true, TArgs_gin_consistent_jsonb_Policy>; }
struct TArgs_gin_consistent_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
TExecFunc arrow_gin_consistent_jsonb_path() { return &GenericExec<TPgDirectFunc<&gin_consistent_jsonb_path>, true, true, TArgs_gin_consistent_jsonb_path_Policy>; }
struct TArgs_gin_extract_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_jsonb() { return &GenericExec<TPgDirectFunc<&gin_extract_jsonb>, true, true, TArgs_gin_extract_jsonb_Policy>; }
struct TArgs_gin_extract_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_gin_extract_jsonb_path() { return &GenericExec<TPgDirectFunc<&gin_extract_jsonb_path>, true, true, TArgs_gin_extract_jsonb_path_Policy>; }
struct TArgs_gin_extract_jsonb_query_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_jsonb_query() { return &GenericExec<TPgDirectFunc<&gin_extract_jsonb_query>, true, true, TArgs_gin_extract_jsonb_query_Policy>; }
struct TArgs_gin_extract_jsonb_query_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_gin_extract_jsonb_query_path() { return &GenericExec<TPgDirectFunc<&gin_extract_jsonb_query_path>, true, true, TArgs_gin_extract_jsonb_query_path_Policy>; }
struct TArgs_gin_triconsistent_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_triconsistent_jsonb() { return &GenericExec<TPgDirectFunc<&gin_triconsistent_jsonb>, true, true, TArgs_gin_triconsistent_jsonb_Policy>; }
struct TArgs_gin_triconsistent_jsonb_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
TExecFunc arrow_gin_triconsistent_jsonb_path() { return &GenericExec<TPgDirectFunc<&gin_triconsistent_jsonb_path>, true, true, TArgs_gin_triconsistent_jsonb_path_Policy>; }
struct TArgs_gisthandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gisthandler() { return &GenericExec<TPgDirectFunc<&gisthandler>, true, true, TArgs_gisthandler_Policy>; }
struct TArgs_gtsquery_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gtsquery_compress() { return &GenericExec<TPgDirectFunc<&gtsquery_compress>, true, true, TArgs_gtsquery_compress_Policy>; }
struct TArgs_gtsquery_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gtsquery_consistent() { return &GenericExec<TPgDirectFunc<&gtsquery_consistent>, true, true, TArgs_gtsquery_consistent_Policy>; }
struct TArgs_gtsquery_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_gtsquery_consistent_oldsig() { return &GenericExec<TPgDirectFunc<&gtsquery_consistent_oldsig>, true, true, TArgs_gtsquery_consistent_oldsig_Policy>; }
struct TArgs_gtsquery_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gtsquery_penalty() { return &GenericExec<TPgDirectFunc<&gtsquery_penalty>, true, true, TArgs_gtsquery_penalty_Policy>; }
struct TArgs_gtsquery_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsquery_picksplit() { return &GenericExec<TPgDirectFunc<&gtsquery_picksplit>, true, true, TArgs_gtsquery_picksplit_Policy>; }
struct TArgs_gtsquery_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gtsquery_same() { return &GenericExec<TPgDirectFunc<&gtsquery_same>, true, true, TArgs_gtsquery_same_Policy>; }
struct TArgs_gtsquery_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsquery_union() { return &GenericExec<TPgDirectFunc<&gtsquery_union>, true, true, TArgs_gtsquery_union_Policy>; }
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
struct TArgs_hash_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_record() { return &GenericExec<TPgDirectFunc<&hash_record>, true, true, TArgs_hash_record_Policy>; }
struct TArgs_hash_record_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_record_extended() { return &GenericExec<TPgDirectFunc<&hash_record_extended>, true, true, TArgs_hash_record_extended_Policy>; }
struct TArgs_hashhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashhandler() { return &GenericExec<TPgDirectFunc<&hashhandler>, true, true, TArgs_hashhandler_Policy>; }
struct TArgs_hashmacaddr8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashmacaddr8() { return &GenericExec<TPgDirectFunc<&hashmacaddr8>, true, true, TArgs_hashmacaddr8_Policy>; }
struct TArgs_hashmacaddr8extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashmacaddr8extended() { return &GenericExec<TPgDirectFunc<&hashmacaddr8extended>, true, true, TArgs_hashmacaddr8extended_Policy>; }
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
struct TArgs_i8tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tod() { return &GenericExec<TPgDirectFunc<&i8tod>, true, true, TArgs_i8tod_Policy>; }
struct TArgs_i8tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tof() { return &GenericExec<TPgDirectFunc<&i8tof>, true, true, TArgs_i8tof_Policy>; }
struct TArgs_i8tooid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i8tooid() { return &GenericExec<TPgDirectFunc<&i8tooid>, true, true, TArgs_i8tooid_Policy>; }
struct TArgs_in_range_date_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_date_interval() { return &GenericExec<TPgDirectFunc<&in_range_date_interval>, true, true, TArgs_in_range_date_interval_Policy>; }
struct TArgs_in_range_int8_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int8_int8() { return &GenericExec<TPgDirectFunc<&in_range_int8_int8>, true, true, TArgs_in_range_int8_int8_Policy>; }
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
struct TArgs_int28_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int28() { return &GenericExec<TPgDirectFunc<&int28>, true, true, TArgs_int28_Policy>; }
struct TArgs_int28div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28div() { return &GenericExec<TPgDirectFunc<&int28div>, true, true, TArgs_int28div_Policy>; }
struct TArgs_int28eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28eq() { return &GenericExec<TPgDirectFunc<&int28eq>, true, true, TArgs_int28eq_Policy>; }
struct TArgs_int28ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28ge() { return &GenericExec<TPgDirectFunc<&int28ge>, true, true, TArgs_int28ge_Policy>; }
struct TArgs_int28gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28gt() { return &GenericExec<TPgDirectFunc<&int28gt>, true, true, TArgs_int28gt_Policy>; }
struct TArgs_int28le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28le() { return &GenericExec<TPgDirectFunc<&int28le>, true, true, TArgs_int28le_Policy>; }
struct TArgs_int28lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28lt() { return &GenericExec<TPgDirectFunc<&int28lt>, true, true, TArgs_int28lt_Policy>; }
struct TArgs_int28mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28mi() { return &GenericExec<TPgDirectFunc<&int28mi>, true, true, TArgs_int28mi_Policy>; }
struct TArgs_int28mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28mul() { return &GenericExec<TPgDirectFunc<&int28mul>, true, true, TArgs_int28mul_Policy>; }
struct TArgs_int28ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28ne() { return &GenericExec<TPgDirectFunc<&int28ne>, true, true, TArgs_int28ne_Policy>; }
struct TArgs_int28pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int28pl() { return &GenericExec<TPgDirectFunc<&int28pl>, true, true, TArgs_int28pl_Policy>; }
struct TArgs_int48_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int48() { return &GenericExec<TPgDirectFunc<&int48>, true, true, TArgs_int48_Policy>; }
struct TArgs_int48div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48div() { return &GenericExec<TPgDirectFunc<&int48div>, true, true, TArgs_int48div_Policy>; }
struct TArgs_int48eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48eq() { return &GenericExec<TPgDirectFunc<&int48eq>, true, true, TArgs_int48eq_Policy>; }
struct TArgs_int48ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48ge() { return &GenericExec<TPgDirectFunc<&int48ge>, true, true, TArgs_int48ge_Policy>; }
struct TArgs_int48gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48gt() { return &GenericExec<TPgDirectFunc<&int48gt>, true, true, TArgs_int48gt_Policy>; }
struct TArgs_int48le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48le() { return &GenericExec<TPgDirectFunc<&int48le>, true, true, TArgs_int48le_Policy>; }
struct TArgs_int48lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48lt() { return &GenericExec<TPgDirectFunc<&int48lt>, true, true, TArgs_int48lt_Policy>; }
struct TArgs_int48mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48mi() { return &GenericExec<TPgDirectFunc<&int48mi>, true, true, TArgs_int48mi_Policy>; }
struct TArgs_int48mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48mul() { return &GenericExec<TPgDirectFunc<&int48mul>, true, true, TArgs_int48mul_Policy>; }
struct TArgs_int48ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48ne() { return &GenericExec<TPgDirectFunc<&int48ne>, true, true, TArgs_int48ne_Policy>; }
struct TArgs_int48pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int48pl() { return &GenericExec<TPgDirectFunc<&int48pl>, true, true, TArgs_int48pl_Policy>; }
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
struct TArgs_int82_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int82() { return &GenericExec<TPgDirectFunc<&int82>, true, true, TArgs_int82_Policy>; }
struct TArgs_int82div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82div() { return &GenericExec<TPgDirectFunc<&int82div>, true, true, TArgs_int82div_Policy>; }
struct TArgs_int82eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82eq() { return &GenericExec<TPgDirectFunc<&int82eq>, true, true, TArgs_int82eq_Policy>; }
struct TArgs_int82ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82ge() { return &GenericExec<TPgDirectFunc<&int82ge>, true, true, TArgs_int82ge_Policy>; }
struct TArgs_int82gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82gt() { return &GenericExec<TPgDirectFunc<&int82gt>, true, true, TArgs_int82gt_Policy>; }
struct TArgs_int82le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82le() { return &GenericExec<TPgDirectFunc<&int82le>, true, true, TArgs_int82le_Policy>; }
struct TArgs_int82lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82lt() { return &GenericExec<TPgDirectFunc<&int82lt>, true, true, TArgs_int82lt_Policy>; }
struct TArgs_int82mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82mi() { return &GenericExec<TPgDirectFunc<&int82mi>, true, true, TArgs_int82mi_Policy>; }
struct TArgs_int82mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82mul() { return &GenericExec<TPgDirectFunc<&int82mul>, true, true, TArgs_int82mul_Policy>; }
struct TArgs_int82ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82ne() { return &GenericExec<TPgDirectFunc<&int82ne>, true, true, TArgs_int82ne_Policy>; }
struct TArgs_int82pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int82pl() { return &GenericExec<TPgDirectFunc<&int82pl>, true, true, TArgs_int82pl_Policy>; }
struct TArgs_int84_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int84() { return &GenericExec<TPgDirectFunc<&int84>, true, true, TArgs_int84_Policy>; }
struct TArgs_int84div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84div() { return &GenericExec<TPgDirectFunc<&int84div>, true, true, TArgs_int84div_Policy>; }
struct TArgs_int84eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84eq() { return &GenericExec<TPgDirectFunc<&int84eq>, true, true, TArgs_int84eq_Policy>; }
struct TArgs_int84ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84ge() { return &GenericExec<TPgDirectFunc<&int84ge>, true, true, TArgs_int84ge_Policy>; }
struct TArgs_int84gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84gt() { return &GenericExec<TPgDirectFunc<&int84gt>, true, true, TArgs_int84gt_Policy>; }
struct TArgs_int84le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84le() { return &GenericExec<TPgDirectFunc<&int84le>, true, true, TArgs_int84le_Policy>; }
struct TArgs_int84lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84lt() { return &GenericExec<TPgDirectFunc<&int84lt>, true, true, TArgs_int84lt_Policy>; }
struct TArgs_int84mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84mi() { return &GenericExec<TPgDirectFunc<&int84mi>, true, true, TArgs_int84mi_Policy>; }
struct TArgs_int84mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84mul() { return &GenericExec<TPgDirectFunc<&int84mul>, true, true, TArgs_int84mul_Policy>; }
struct TArgs_int84ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84ne() { return &GenericExec<TPgDirectFunc<&int84ne>, true, true, TArgs_int84ne_Policy>; }
struct TArgs_int84pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int84pl() { return &GenericExec<TPgDirectFunc<&int84pl>, true, true, TArgs_int84pl_Policy>; }
struct TArgs_int8abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8abs() { return &GenericExec<TPgDirectFunc<&int8abs>, true, true, TArgs_int8abs_Policy>; }
struct TArgs_int8and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8and() { return &GenericExec<TPgDirectFunc<&int8and>, true, true, TArgs_int8and_Policy>; }
struct TArgs_int8dec_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8dec() { return &GenericExec<TPgDirectFunc<&int8dec>, true, true, TArgs_int8dec_Policy>; }
struct TArgs_int8dec_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8dec_any() { return &GenericExec<TPgDirectFunc<&int8dec_any>, true, true, TArgs_int8dec_any_Policy>; }
struct TArgs_int8div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8div() { return &GenericExec<TPgDirectFunc<&int8div>, true, true, TArgs_int8div_Policy>; }
struct TArgs_int8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8eq() { return &GenericExec<TPgDirectFunc<&int8eq>, true, true, TArgs_int8eq_Policy>; }
struct TArgs_int8gcd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8gcd() { return &GenericExec<TPgDirectFunc<&int8gcd>, true, true, TArgs_int8gcd_Policy>; }
struct TArgs_int8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8ge() { return &GenericExec<TPgDirectFunc<&int8ge>, true, true, TArgs_int8ge_Policy>; }
struct TArgs_int8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8gt() { return &GenericExec<TPgDirectFunc<&int8gt>, true, true, TArgs_int8gt_Policy>; }
struct TArgs_int8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int8in() { return &GenericExec<TPgDirectFunc<&int8in>, true, true, TArgs_int8in_Policy>; }
struct TArgs_int8inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8inc() { return &GenericExec<TPgDirectFunc<&int8inc>, true, true, TArgs_int8inc_Policy>; }
struct TArgs_int8inc_any_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8inc_any() { return &GenericExec<TPgDirectFunc<&int8inc_any>, true, true, TArgs_int8inc_any_Policy>; }
struct TArgs_int8inc_float8_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_int8inc_float8_float8() { return &GenericExec<TPgDirectFunc<&int8inc_float8_float8>, true, true, TArgs_int8inc_float8_float8_Policy>; }
struct TArgs_int8larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8larger() { return &GenericExec<TPgDirectFunc<&int8larger>, true, true, TArgs_int8larger_Policy>; }
struct TArgs_int8lcm_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8lcm() { return &GenericExec<TPgDirectFunc<&int8lcm>, true, true, TArgs_int8lcm_Policy>; }
struct TArgs_int8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8le() { return &GenericExec<TPgDirectFunc<&int8le>, true, true, TArgs_int8le_Policy>; }
struct TArgs_int8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8lt() { return &GenericExec<TPgDirectFunc<&int8lt>, true, true, TArgs_int8lt_Policy>; }
struct TArgs_int8mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mi() { return &GenericExec<TPgDirectFunc<&int8mi>, true, true, TArgs_int8mi_Policy>; }
struct TArgs_int8mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mod() { return &GenericExec<TPgDirectFunc<&int8mod>, true, true, TArgs_int8mod_Policy>; }
struct TArgs_int8mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8mul() { return &GenericExec<TPgDirectFunc<&int8mul>, true, true, TArgs_int8mul_Policy>; }
struct TArgs_int8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8ne() { return &GenericExec<TPgDirectFunc<&int8ne>, true, true, TArgs_int8ne_Policy>; }
struct TArgs_int8not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8not() { return &GenericExec<TPgDirectFunc<&int8not>, true, true, TArgs_int8not_Policy>; }
struct TArgs_int8or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8or() { return &GenericExec<TPgDirectFunc<&int8or>, true, true, TArgs_int8or_Policy>; }
struct TArgs_int8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8out() { return &GenericExec<TPgDirectFunc<&int8out>, true, false, TArgs_int8out_Policy>; }
struct TArgs_int8pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8pl() { return &GenericExec<TPgDirectFunc<&int8pl>, true, true, TArgs_int8pl_Policy>; }
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
struct TArgs_int8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8recv() { return &GenericExec<TPgDirectFunc<&int8recv>, true, true, TArgs_int8recv_Policy>; }
struct TArgs_int8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8send() { return &GenericExec<TPgDirectFunc<&int8send>, true, false, TArgs_int8send_Policy>; }
struct TArgs_int8shl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8shl() { return &GenericExec<TPgDirectFunc<&int8shl>, true, true, TArgs_int8shl_Policy>; }
struct TArgs_int8shr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8shr() { return &GenericExec<TPgDirectFunc<&int8shr>, true, true, TArgs_int8shr_Policy>; }
struct TArgs_int8smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8smaller() { return &GenericExec<TPgDirectFunc<&int8smaller>, true, true, TArgs_int8smaller_Policy>; }
struct TArgs_int8um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8um() { return &GenericExec<TPgDirectFunc<&int8um>, true, true, TArgs_int8um_Policy>; }
struct TArgs_int8up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8up() { return &GenericExec<TPgDirectFunc<&int8up>, true, true, TArgs_int8up_Policy>; }
struct TArgs_int8xor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8xor() { return &GenericExec<TPgDirectFunc<&int8xor>, true, true, TArgs_int8xor_Policy>; }
struct TArgs_interval_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_time() { return &GenericExec<TPgDirectFunc<&interval_time>, true, true, TArgs_interval_time_Policy>; }
struct TArgs_lastval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_lastval() { return &GenericExec<TPgDirectFunc<&lastval>, true, true, TArgs_lastval_Policy>; }
struct TArgs_macaddr8_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_and() { return &GenericExec<TPgDirectFunc<&macaddr8_and>, true, false, TArgs_macaddr8_and_Policy>; }
struct TArgs_macaddr8_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_cmp() { return &GenericExec<TPgDirectFunc<&macaddr8_cmp>, true, true, TArgs_macaddr8_cmp_Policy>; }
struct TArgs_macaddr8_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_eq() { return &GenericExec<TPgDirectFunc<&macaddr8_eq>, true, true, TArgs_macaddr8_eq_Policy>; }
struct TArgs_macaddr8_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_ge() { return &GenericExec<TPgDirectFunc<&macaddr8_ge>, true, true, TArgs_macaddr8_ge_Policy>; }
struct TArgs_macaddr8_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_gt() { return &GenericExec<TPgDirectFunc<&macaddr8_gt>, true, true, TArgs_macaddr8_gt_Policy>; }
struct TArgs_macaddr8_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_in() { return &GenericExec<TPgDirectFunc<&macaddr8_in>, true, false, TArgs_macaddr8_in_Policy>; }
struct TArgs_macaddr8_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_le() { return &GenericExec<TPgDirectFunc<&macaddr8_le>, true, true, TArgs_macaddr8_le_Policy>; }
struct TArgs_macaddr8_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_lt() { return &GenericExec<TPgDirectFunc<&macaddr8_lt>, true, true, TArgs_macaddr8_lt_Policy>; }
struct TArgs_macaddr8_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_ne() { return &GenericExec<TPgDirectFunc<&macaddr8_ne>, true, true, TArgs_macaddr8_ne_Policy>; }
struct TArgs_macaddr8_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_not() { return &GenericExec<TPgDirectFunc<&macaddr8_not>, true, false, TArgs_macaddr8_not_Policy>; }
struct TArgs_macaddr8_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr8_or() { return &GenericExec<TPgDirectFunc<&macaddr8_or>, true, false, TArgs_macaddr8_or_Policy>; }
struct TArgs_macaddr8_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_out() { return &GenericExec<TPgDirectFunc<&macaddr8_out>, true, false, TArgs_macaddr8_out_Policy>; }
struct TArgs_macaddr8_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_macaddr8_recv() { return &GenericExec<TPgDirectFunc<&macaddr8_recv>, true, false, TArgs_macaddr8_recv_Policy>; }
struct TArgs_macaddr8_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_send() { return &GenericExec<TPgDirectFunc<&macaddr8_send>, true, false, TArgs_macaddr8_send_Policy>; }
struct TArgs_macaddr8_set7bit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_set7bit() { return &GenericExec<TPgDirectFunc<&macaddr8_set7bit>, true, false, TArgs_macaddr8_set7bit_Policy>; }
struct TArgs_macaddr8_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8_trunc() { return &GenericExec<TPgDirectFunc<&macaddr8_trunc>, true, false, TArgs_macaddr8_trunc_Policy>; }
struct TArgs_macaddr8tomacaddr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr8tomacaddr() { return &GenericExec<TPgDirectFunc<&macaddr8tomacaddr>, true, false, TArgs_macaddr8tomacaddr_Policy>; }
struct TArgs_macaddrtomacaddr8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddrtomacaddr8() { return &GenericExec<TPgDirectFunc<&macaddrtomacaddr8>, true, false, TArgs_macaddrtomacaddr8_Policy>; }
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
struct TArgs_multirange_gist_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_multirange_gist_compress() { return &GenericExec<TPgDirectFunc<&multirange_gist_compress>, true, true, TArgs_multirange_gist_compress_Policy>; }
struct TArgs_multirange_gist_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_multirange_gist_consistent() { return &GenericExec<TPgDirectFunc<&multirange_gist_consistent>, true, true, TArgs_multirange_gist_consistent_Policy>; }
struct TArgs_mxid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_mxid_age() { return &GenericExec<TPgDirectFunc<&mxid_age>, true, true, TArgs_mxid_age_Policy>; }
struct TArgs_nameconcatoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_nameconcatoid() { return &GenericExec<TPgDirectFunc<&nameconcatoid>, true, false, TArgs_nameconcatoid_Policy>; }
struct TArgs_nameeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameeq() { return &GenericExec<TPgDirectFunc<&nameeq>, true, true, TArgs_nameeq_Policy>; }
struct TArgs_namege_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namege() { return &GenericExec<TPgDirectFunc<&namege>, true, true, TArgs_namege_Policy>; }
struct TArgs_namegt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegt() { return &GenericExec<TPgDirectFunc<&namegt>, true, true, TArgs_namegt_Policy>; }
struct TArgs_namein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_namein() { return &GenericExec<TPgDirectFunc<&namein>, true, false, TArgs_namein_Policy>; }
struct TArgs_namele_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namele() { return &GenericExec<TPgDirectFunc<&namele>, true, true, TArgs_namele_Policy>; }
struct TArgs_namelt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelt() { return &GenericExec<TPgDirectFunc<&namelt>, true, true, TArgs_namelt_Policy>; }
struct TArgs_namene_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namene() { return &GenericExec<TPgDirectFunc<&namene>, true, true, TArgs_namene_Policy>; }
struct TArgs_nameout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_nameout() { return &GenericExec<TPgDirectFunc<&nameout>, true, false, TArgs_nameout_Policy>; }
struct TArgs_namerecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_namerecv() { return &GenericExec<TPgDirectFunc<&namerecv>, true, false, TArgs_namerecv_Policy>; }
struct TArgs_namesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_namesend() { return &GenericExec<TPgDirectFunc<&namesend>, true, false, TArgs_namesend_Policy>; }
struct TArgs_nextval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_nextval_oid() { return &GenericExec<TPgDirectFunc<&nextval_oid>, true, true, TArgs_nextval_oid_Policy>; }
struct TArgs_numrange_subdiff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numrange_subdiff() { return &GenericExec<TPgDirectFunc<&numrange_subdiff>, true, true, TArgs_numrange_subdiff_Policy>; }
struct TArgs_oidtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidtoi8() { return &GenericExec<TPgDirectFunc<&oidtoi8>, true, true, TArgs_oidtoi8_Policy>; }
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
struct TArgs_pg_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backend_pid() { return &GenericExec<TPgDirectFunc<&pg_backend_pid>, true, true, TArgs_pg_backend_pid_Policy>; }
struct TArgs_pg_collation_actual_version_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_actual_version() { return &GenericExec<TPgDirectFunc<&pg_collation_actual_version>, true, false, TArgs_pg_collation_actual_version_Policy>; }
struct TArgs_pg_collation_for_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_for() { return &GenericExec<TPgDirectFunc<&pg_collation_for>, false, false, TArgs_pg_collation_for_Policy>; }
struct TArgs_pg_column_is_updatable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_column_is_updatable() { return &GenericExec<TPgDirectFunc<&pg_column_is_updatable>, true, true, TArgs_pg_column_is_updatable_Policy>; }
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
struct TArgs_pg_database_size_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_database_size_name() { return &GenericExec<TPgDirectFunc<&pg_database_size_name>, true, true, TArgs_pg_database_size_name_Policy>; }
struct TArgs_pg_database_size_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_database_size_oid() { return &GenericExec<TPgDirectFunc<&pg_database_size_oid>, true, true, TArgs_pg_database_size_oid_Policy>; }
struct TArgs_pg_filenode_relation_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_filenode_relation() { return &GenericExec<TPgDirectFunc<&pg_filenode_relation>, true, true, TArgs_pg_filenode_relation_Policy>; }
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
struct TArgs_pg_import_system_collations_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_import_system_collations() { return &GenericExec<TPgDirectFunc<&pg_import_system_collations>, true, true, TArgs_pg_import_system_collations_Policy>; }
struct TArgs_pg_indexes_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_indexes_size() { return &GenericExec<TPgDirectFunc<&pg_indexes_size>, true, true, TArgs_pg_indexes_size_Policy>; }
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
struct TArgs_pg_read_binary_file_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,true};
};
TExecFunc arrow_pg_read_binary_file() { return &GenericExec<TPgDirectFunc<&pg_read_binary_file>, true, false, TArgs_pg_read_binary_file_Policy>; }
struct TArgs_pg_read_binary_file_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_read_binary_file_all() { return &GenericExec<TPgDirectFunc<&pg_read_binary_file_all>, true, false, TArgs_pg_read_binary_file_all_Policy>; }
struct TArgs_pg_read_binary_file_off_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_read_binary_file_off_len() { return &GenericExec<TPgDirectFunc<&pg_read_binary_file_off_len>, true, false, TArgs_pg_read_binary_file_off_len_Policy>; }
struct TArgs_pg_read_file_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_read_file() { return &GenericExec<TPgDirectFunc<&pg_read_file>, true, false, TArgs_pg_read_file_Policy>; }
struct TArgs_pg_read_file_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_read_file_all() { return &GenericExec<TPgDirectFunc<&pg_read_file_all>, true, false, TArgs_pg_read_file_all_Policy>; }
struct TArgs_pg_read_file_off_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_read_file_off_len() { return &GenericExec<TPgDirectFunc<&pg_read_file_off_len>, true, false, TArgs_pg_read_file_off_len_Policy>; }
struct TArgs_pg_read_file_v2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,true};
};
TExecFunc arrow_pg_read_file_v2() { return &GenericExec<TPgDirectFunc<&pg_read_file_v2>, true, false, TArgs_pg_read_file_v2_Policy>; }
struct TArgs_pg_relation_filenode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_filenode() { return &GenericExec<TPgDirectFunc<&pg_relation_filenode>, true, true, TArgs_pg_relation_filenode_Policy>; }
struct TArgs_pg_relation_filepath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_filepath() { return &GenericExec<TPgDirectFunc<&pg_relation_filepath>, true, false, TArgs_pg_relation_filepath_Policy>; }
struct TArgs_pg_relation_is_updatable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_relation_is_updatable() { return &GenericExec<TPgDirectFunc<&pg_relation_is_updatable>, true, true, TArgs_pg_relation_is_updatable_Policy>; }
struct TArgs_pg_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_relation_size() { return &GenericExec<TPgDirectFunc<&pg_relation_size>, true, true, TArgs_pg_relation_size_Policy>; }
struct TArgs_pg_replication_origin_advance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_replication_origin_advance() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_advance>, true, true, TArgs_pg_replication_origin_advance_Policy>; }
struct TArgs_pg_replication_origin_create_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_replication_origin_create() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_create>, true, true, TArgs_pg_replication_origin_create_Policy>; }
struct TArgs_pg_replication_origin_drop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_replication_origin_drop() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_drop>, true, true, TArgs_pg_replication_origin_drop_Policy>; }
struct TArgs_pg_replication_origin_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_replication_origin_oid() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_oid>, true, true, TArgs_pg_replication_origin_oid_Policy>; }
struct TArgs_pg_replication_origin_progress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_replication_origin_progress() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_progress>, true, true, TArgs_pg_replication_origin_progress_Policy>; }
struct TArgs_pg_replication_origin_session_is_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_replication_origin_session_is_setup() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_session_is_setup>, true, true, TArgs_pg_replication_origin_session_is_setup_Policy>; }
struct TArgs_pg_replication_origin_session_progress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_replication_origin_session_progress() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_session_progress>, true, true, TArgs_pg_replication_origin_session_progress_Policy>; }
struct TArgs_pg_replication_origin_session_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_replication_origin_session_reset() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_session_reset>, true, true, TArgs_pg_replication_origin_session_reset_Policy>; }
struct TArgs_pg_replication_origin_session_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_replication_origin_session_setup() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_session_setup>, true, true, TArgs_pg_replication_origin_session_setup_Policy>; }
struct TArgs_pg_replication_origin_xact_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_replication_origin_xact_reset() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_xact_reset>, true, true, TArgs_pg_replication_origin_xact_reset_Policy>; }
struct TArgs_pg_replication_origin_xact_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_replication_origin_xact_setup() { return &GenericExec<TPgDirectFunc<&pg_replication_origin_xact_setup>, true, true, TArgs_pg_replication_origin_xact_setup_Policy>; }
struct TArgs_pg_sequence_last_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sequence_last_value() { return &GenericExec<TPgDirectFunc<&pg_sequence_last_value>, true, true, TArgs_pg_sequence_last_value_Policy>; }
struct TArgs_pg_sequence_parameters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sequence_parameters() { return &GenericExec<TPgDirectFunc<&pg_sequence_parameters>, true, false, TArgs_pg_sequence_parameters_Policy>; }
struct TArgs_pg_size_bytes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_size_bytes() { return &GenericExec<TPgDirectFunc<&pg_size_bytes>, true, true, TArgs_pg_size_bytes_Policy>; }
struct TArgs_pg_size_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_size_pretty() { return &GenericExec<TPgDirectFunc<&pg_size_pretty>, true, false, TArgs_pg_size_pretty_Policy>; }
struct TArgs_pg_size_pretty_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_size_pretty_numeric() { return &GenericExec<TPgDirectFunc<&pg_size_pretty_numeric>, true, false, TArgs_pg_size_pretty_numeric_Policy>; }
struct TArgs_pg_sleep_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sleep() { return &GenericExec<TPgDirectFunc<&pg_sleep>, true, true, TArgs_pg_sleep_Policy>; }
struct TArgs_pg_stat_clear_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_clear_snapshot() { return &GenericExec<TPgDirectFunc<&pg_stat_clear_snapshot>, false, true, TArgs_pg_stat_clear_snapshot_Policy>; }
struct TArgs_pg_stat_file_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_stat_file() { return &GenericExec<TPgDirectFunc<&pg_stat_file>, true, false, TArgs_pg_stat_file_Policy>; }
struct TArgs_pg_stat_file_1arg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_file_1arg() { return &GenericExec<TPgDirectFunc<&pg_stat_file_1arg>, true, false, TArgs_pg_stat_file_1arg_Policy>; }
struct TArgs_pg_stat_get_analyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_analyze_count() { return &GenericExec<TPgDirectFunc<&pg_stat_get_analyze_count>, true, true, TArgs_pg_stat_get_analyze_count_Policy>; }
struct TArgs_pg_stat_get_archiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_archiver() { return &GenericExec<TPgDirectFunc<&pg_stat_get_archiver>, false, false, TArgs_pg_stat_get_archiver_Policy>; }
struct TArgs_pg_stat_get_autoanalyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_autoanalyze_count() { return &GenericExec<TPgDirectFunc<&pg_stat_get_autoanalyze_count>, true, true, TArgs_pg_stat_get_autoanalyze_count_Policy>; }
struct TArgs_pg_stat_get_autovacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_autovacuum_count() { return &GenericExec<TPgDirectFunc<&pg_stat_get_autovacuum_count>, true, true, TArgs_pg_stat_get_autovacuum_count_Policy>; }
struct TArgs_pg_stat_get_backend_activity_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_activity() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_activity>, true, false, TArgs_pg_stat_get_backend_activity_Policy>; }
struct TArgs_pg_stat_get_backend_activity_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_activity_start() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_activity_start>, true, true, TArgs_pg_stat_get_backend_activity_start_Policy>; }
struct TArgs_pg_stat_get_backend_client_addr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_client_addr() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_client_addr>, true, false, TArgs_pg_stat_get_backend_client_addr_Policy>; }
struct TArgs_pg_stat_get_backend_client_port_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_client_port() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_client_port>, true, true, TArgs_pg_stat_get_backend_client_port_Policy>; }
struct TArgs_pg_stat_get_backend_dbid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_dbid() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_dbid>, true, true, TArgs_pg_stat_get_backend_dbid_Policy>; }
struct TArgs_pg_stat_get_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_pid() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_pid>, true, true, TArgs_pg_stat_get_backend_pid_Policy>; }
struct TArgs_pg_stat_get_backend_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_start() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_start>, true, true, TArgs_pg_stat_get_backend_start_Policy>; }
struct TArgs_pg_stat_get_backend_userid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_userid() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_userid>, true, true, TArgs_pg_stat_get_backend_userid_Policy>; }
struct TArgs_pg_stat_get_backend_wait_event_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_wait_event() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_wait_event>, true, false, TArgs_pg_stat_get_backend_wait_event_Policy>; }
struct TArgs_pg_stat_get_backend_wait_event_type_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_wait_event_type() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_wait_event_type>, true, false, TArgs_pg_stat_get_backend_wait_event_type_Policy>; }
struct TArgs_pg_stat_get_backend_xact_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_xact_start() { return &GenericExec<TPgDirectFunc<&pg_stat_get_backend_xact_start>, true, true, TArgs_pg_stat_get_backend_xact_start_Policy>; }
struct TArgs_pg_stat_get_bgwriter_buf_written_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_buf_written_checkpoints() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_buf_written_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_buf_written_checkpoints_Policy>; }
struct TArgs_pg_stat_get_bgwriter_buf_written_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_buf_written_clean() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_buf_written_clean>, true, true, TArgs_pg_stat_get_bgwriter_buf_written_clean_Policy>; }
struct TArgs_pg_stat_get_bgwriter_maxwritten_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_maxwritten_clean() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_maxwritten_clean>, true, true, TArgs_pg_stat_get_bgwriter_maxwritten_clean_Policy>; }
struct TArgs_pg_stat_get_bgwriter_requested_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_requested_checkpoints() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_requested_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_requested_checkpoints_Policy>; }
struct TArgs_pg_stat_get_bgwriter_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_stat_reset_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_stat_reset_time>, true, true, TArgs_pg_stat_get_bgwriter_stat_reset_time_Policy>; }
struct TArgs_pg_stat_get_bgwriter_timed_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_timed_checkpoints() { return &GenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_timed_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_timed_checkpoints_Policy>; }
struct TArgs_pg_stat_get_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_blocks_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_blocks_fetched>, true, true, TArgs_pg_stat_get_blocks_fetched_Policy>; }
struct TArgs_pg_stat_get_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_blocks_hit() { return &GenericExec<TPgDirectFunc<&pg_stat_get_blocks_hit>, true, true, TArgs_pg_stat_get_blocks_hit_Policy>; }
struct TArgs_pg_stat_get_buf_alloc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_alloc() { return &GenericExec<TPgDirectFunc<&pg_stat_get_buf_alloc>, true, true, TArgs_pg_stat_get_buf_alloc_Policy>; }
struct TArgs_pg_stat_get_buf_fsync_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_fsync_backend() { return &GenericExec<TPgDirectFunc<&pg_stat_get_buf_fsync_backend>, true, true, TArgs_pg_stat_get_buf_fsync_backend_Policy>; }
struct TArgs_pg_stat_get_buf_written_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_written_backend() { return &GenericExec<TPgDirectFunc<&pg_stat_get_buf_written_backend>, true, true, TArgs_pg_stat_get_buf_written_backend_Policy>; }
struct TArgs_pg_stat_get_checkpoint_sync_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_checkpoint_sync_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_checkpoint_sync_time>, true, true, TArgs_pg_stat_get_checkpoint_sync_time_Policy>; }
struct TArgs_pg_stat_get_checkpoint_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_checkpoint_write_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_checkpoint_write_time>, true, true, TArgs_pg_stat_get_checkpoint_write_time_Policy>; }
struct TArgs_pg_stat_get_db_active_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_active_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_active_time>, true, true, TArgs_pg_stat_get_db_active_time_Policy>; }
struct TArgs_pg_stat_get_db_blk_read_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blk_read_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_blk_read_time>, true, true, TArgs_pg_stat_get_db_blk_read_time_Policy>; }
struct TArgs_pg_stat_get_db_blk_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blk_write_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_blk_write_time>, true, true, TArgs_pg_stat_get_db_blk_write_time_Policy>; }
struct TArgs_pg_stat_get_db_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blocks_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_blocks_fetched>, true, true, TArgs_pg_stat_get_db_blocks_fetched_Policy>; }
struct TArgs_pg_stat_get_db_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blocks_hit() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_blocks_hit>, true, true, TArgs_pg_stat_get_db_blocks_hit_Policy>; }
struct TArgs_pg_stat_get_db_checksum_failures_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_checksum_failures() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_checksum_failures>, true, true, TArgs_pg_stat_get_db_checksum_failures_Policy>; }
struct TArgs_pg_stat_get_db_checksum_last_failure_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_checksum_last_failure() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_checksum_last_failure>, true, true, TArgs_pg_stat_get_db_checksum_last_failure_Policy>; }
struct TArgs_pg_stat_get_db_conflict_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_all() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_all>, true, true, TArgs_pg_stat_get_db_conflict_all_Policy>; }
struct TArgs_pg_stat_get_db_conflict_bufferpin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_bufferpin() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_bufferpin>, true, true, TArgs_pg_stat_get_db_conflict_bufferpin_Policy>; }
struct TArgs_pg_stat_get_db_conflict_lock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_lock() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_lock>, true, true, TArgs_pg_stat_get_db_conflict_lock_Policy>; }
struct TArgs_pg_stat_get_db_conflict_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_snapshot() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_snapshot>, true, true, TArgs_pg_stat_get_db_conflict_snapshot_Policy>; }
struct TArgs_pg_stat_get_db_conflict_startup_deadlock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_startup_deadlock() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_startup_deadlock>, true, true, TArgs_pg_stat_get_db_conflict_startup_deadlock_Policy>; }
struct TArgs_pg_stat_get_db_conflict_tablespace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_tablespace() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_tablespace>, true, true, TArgs_pg_stat_get_db_conflict_tablespace_Policy>; }
struct TArgs_pg_stat_get_db_deadlocks_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_deadlocks() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_deadlocks>, true, true, TArgs_pg_stat_get_db_deadlocks_Policy>; }
struct TArgs_pg_stat_get_db_idle_in_transaction_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_idle_in_transaction_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_idle_in_transaction_time>, true, true, TArgs_pg_stat_get_db_idle_in_transaction_time_Policy>; }
struct TArgs_pg_stat_get_db_numbackends_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_numbackends() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_numbackends>, true, true, TArgs_pg_stat_get_db_numbackends_Policy>; }
struct TArgs_pg_stat_get_db_session_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_session_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_session_time>, true, true, TArgs_pg_stat_get_db_session_time_Policy>; }
struct TArgs_pg_stat_get_db_sessions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_sessions>, true, true, TArgs_pg_stat_get_db_sessions_Policy>; }
struct TArgs_pg_stat_get_db_sessions_abandoned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_abandoned() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_abandoned>, true, true, TArgs_pg_stat_get_db_sessions_abandoned_Policy>; }
struct TArgs_pg_stat_get_db_sessions_fatal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_fatal() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_fatal>, true, true, TArgs_pg_stat_get_db_sessions_fatal_Policy>; }
struct TArgs_pg_stat_get_db_sessions_killed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_killed() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_killed>, true, true, TArgs_pg_stat_get_db_sessions_killed_Policy>; }
struct TArgs_pg_stat_get_db_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_stat_reset_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_stat_reset_time>, true, true, TArgs_pg_stat_get_db_stat_reset_time_Policy>; }
struct TArgs_pg_stat_get_db_temp_bytes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_temp_bytes() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_temp_bytes>, true, true, TArgs_pg_stat_get_db_temp_bytes_Policy>; }
struct TArgs_pg_stat_get_db_temp_files_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_temp_files() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_temp_files>, true, true, TArgs_pg_stat_get_db_temp_files_Policy>; }
struct TArgs_pg_stat_get_db_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_deleted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_deleted>, true, true, TArgs_pg_stat_get_db_tuples_deleted_Policy>; }
struct TArgs_pg_stat_get_db_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_fetched>, true, true, TArgs_pg_stat_get_db_tuples_fetched_Policy>; }
struct TArgs_pg_stat_get_db_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_inserted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_inserted>, true, true, TArgs_pg_stat_get_db_tuples_inserted_Policy>; }
struct TArgs_pg_stat_get_db_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_returned() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_returned>, true, true, TArgs_pg_stat_get_db_tuples_returned_Policy>; }
struct TArgs_pg_stat_get_db_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_updated() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_updated>, true, true, TArgs_pg_stat_get_db_tuples_updated_Policy>; }
struct TArgs_pg_stat_get_db_xact_commit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_xact_commit() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_xact_commit>, true, true, TArgs_pg_stat_get_db_xact_commit_Policy>; }
struct TArgs_pg_stat_get_db_xact_rollback_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_xact_rollback() { return &GenericExec<TPgDirectFunc<&pg_stat_get_db_xact_rollback>, true, true, TArgs_pg_stat_get_db_xact_rollback_Policy>; }
struct TArgs_pg_stat_get_dead_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_dead_tuples() { return &GenericExec<TPgDirectFunc<&pg_stat_get_dead_tuples>, true, true, TArgs_pg_stat_get_dead_tuples_Policy>; }
struct TArgs_pg_stat_get_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_calls() { return &GenericExec<TPgDirectFunc<&pg_stat_get_function_calls>, true, true, TArgs_pg_stat_get_function_calls_Policy>; }
struct TArgs_pg_stat_get_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_self_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_function_self_time>, true, true, TArgs_pg_stat_get_function_self_time_Policy>; }
struct TArgs_pg_stat_get_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_total_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_function_total_time>, true, true, TArgs_pg_stat_get_function_total_time_Policy>; }
struct TArgs_pg_stat_get_ins_since_vacuum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_ins_since_vacuum() { return &GenericExec<TPgDirectFunc<&pg_stat_get_ins_since_vacuum>, true, true, TArgs_pg_stat_get_ins_since_vacuum_Policy>; }
struct TArgs_pg_stat_get_last_analyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_analyze_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_last_analyze_time>, true, true, TArgs_pg_stat_get_last_analyze_time_Policy>; }
struct TArgs_pg_stat_get_last_autoanalyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_autoanalyze_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_last_autoanalyze_time>, true, true, TArgs_pg_stat_get_last_autoanalyze_time_Policy>; }
struct TArgs_pg_stat_get_last_autovacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_autovacuum_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_last_autovacuum_time>, true, true, TArgs_pg_stat_get_last_autovacuum_time_Policy>; }
struct TArgs_pg_stat_get_last_vacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_vacuum_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_last_vacuum_time>, true, true, TArgs_pg_stat_get_last_vacuum_time_Policy>; }
struct TArgs_pg_stat_get_live_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_live_tuples() { return &GenericExec<TPgDirectFunc<&pg_stat_get_live_tuples>, true, true, TArgs_pg_stat_get_live_tuples_Policy>; }
struct TArgs_pg_stat_get_mod_since_analyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_mod_since_analyze() { return &GenericExec<TPgDirectFunc<&pg_stat_get_mod_since_analyze>, true, true, TArgs_pg_stat_get_mod_since_analyze_Policy>; }
struct TArgs_pg_stat_get_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_numscans() { return &GenericExec<TPgDirectFunc<&pg_stat_get_numscans>, true, true, TArgs_pg_stat_get_numscans_Policy>; }
struct TArgs_pg_stat_get_snapshot_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_snapshot_timestamp() { return &GenericExec<TPgDirectFunc<&pg_stat_get_snapshot_timestamp>, true, true, TArgs_pg_stat_get_snapshot_timestamp_Policy>; }
struct TArgs_pg_stat_get_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_deleted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_deleted>, true, true, TArgs_pg_stat_get_tuples_deleted_Policy>; }
struct TArgs_pg_stat_get_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_fetched>, true, true, TArgs_pg_stat_get_tuples_fetched_Policy>; }
struct TArgs_pg_stat_get_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_hot_updated() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_hot_updated>, true, true, TArgs_pg_stat_get_tuples_hot_updated_Policy>; }
struct TArgs_pg_stat_get_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_inserted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_inserted>, true, true, TArgs_pg_stat_get_tuples_inserted_Policy>; }
struct TArgs_pg_stat_get_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_returned() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_returned>, true, true, TArgs_pg_stat_get_tuples_returned_Policy>; }
struct TArgs_pg_stat_get_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_updated() { return &GenericExec<TPgDirectFunc<&pg_stat_get_tuples_updated>, true, true, TArgs_pg_stat_get_tuples_updated_Policy>; }
struct TArgs_pg_stat_get_vacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_vacuum_count() { return &GenericExec<TPgDirectFunc<&pg_stat_get_vacuum_count>, true, true, TArgs_pg_stat_get_vacuum_count_Policy>; }
struct TArgs_pg_stat_get_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_wal() { return &GenericExec<TPgDirectFunc<&pg_stat_get_wal>, false, false, TArgs_pg_stat_get_wal_Policy>; }
struct TArgs_pg_stat_get_xact_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_blocks_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_blocks_fetched>, true, true, TArgs_pg_stat_get_xact_blocks_fetched_Policy>; }
struct TArgs_pg_stat_get_xact_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_blocks_hit() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_blocks_hit>, true, true, TArgs_pg_stat_get_xact_blocks_hit_Policy>; }
struct TArgs_pg_stat_get_xact_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_calls() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_function_calls>, true, true, TArgs_pg_stat_get_xact_function_calls_Policy>; }
struct TArgs_pg_stat_get_xact_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_self_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_function_self_time>, true, true, TArgs_pg_stat_get_xact_function_self_time_Policy>; }
struct TArgs_pg_stat_get_xact_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_total_time() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_function_total_time>, true, true, TArgs_pg_stat_get_xact_function_total_time_Policy>; }
struct TArgs_pg_stat_get_xact_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_numscans() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_numscans>, true, true, TArgs_pg_stat_get_xact_numscans_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_deleted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_deleted>, true, true, TArgs_pg_stat_get_xact_tuples_deleted_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_fetched() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_fetched>, true, true, TArgs_pg_stat_get_xact_tuples_fetched_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_hot_updated() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_hot_updated>, true, true, TArgs_pg_stat_get_xact_tuples_hot_updated_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_inserted() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_inserted>, true, true, TArgs_pg_stat_get_xact_tuples_inserted_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_returned() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_returned>, true, true, TArgs_pg_stat_get_xact_tuples_returned_Policy>; }
struct TArgs_pg_stat_get_xact_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_updated() { return &GenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_updated>, true, true, TArgs_pg_stat_get_xact_tuples_updated_Policy>; }
struct TArgs_pg_stat_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_reset() { return &GenericExec<TPgDirectFunc<&pg_stat_reset>, false, true, TArgs_pg_stat_reset_Policy>; }
struct TArgs_pg_stat_reset_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_replication_slot() { return &GenericExec<TPgDirectFunc<&pg_stat_reset_replication_slot>, false, true, TArgs_pg_stat_reset_replication_slot_Policy>; }
struct TArgs_pg_stat_reset_shared_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_shared() { return &GenericExec<TPgDirectFunc<&pg_stat_reset_shared>, true, true, TArgs_pg_stat_reset_shared_Policy>; }
struct TArgs_pg_stat_reset_single_function_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_reset_single_function_counters() { return &GenericExec<TPgDirectFunc<&pg_stat_reset_single_function_counters>, true, true, TArgs_pg_stat_reset_single_function_counters_Policy>; }
struct TArgs_pg_stat_reset_single_table_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_reset_single_table_counters() { return &GenericExec<TPgDirectFunc<&pg_stat_reset_single_table_counters>, true, true, TArgs_pg_stat_reset_single_table_counters_Policy>; }
struct TArgs_pg_stat_reset_slru_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_slru() { return &GenericExec<TPgDirectFunc<&pg_stat_reset_slru>, false, true, TArgs_pg_stat_reset_slru_Policy>; }
struct TArgs_pg_table_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_size() { return &GenericExec<TPgDirectFunc<&pg_table_size>, true, true, TArgs_pg_table_size_Policy>; }
struct TArgs_pg_tablespace_location_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_tablespace_location() { return &GenericExec<TPgDirectFunc<&pg_tablespace_location>, true, false, TArgs_pg_tablespace_location_Policy>; }
struct TArgs_pg_tablespace_size_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_tablespace_size_name() { return &GenericExec<TPgDirectFunc<&pg_tablespace_size_name>, true, true, TArgs_pg_tablespace_size_name_Policy>; }
struct TArgs_pg_tablespace_size_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_tablespace_size_oid() { return &GenericExec<TPgDirectFunc<&pg_tablespace_size_oid>, true, true, TArgs_pg_tablespace_size_oid_Policy>; }
struct TArgs_pg_total_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_total_relation_size() { return &GenericExec<TPgDirectFunc<&pg_total_relation_size>, true, true, TArgs_pg_total_relation_size_Policy>; }
struct TArgs_pg_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_typeof() { return &GenericExec<TPgDirectFunc<&pg_typeof>, false, true, TArgs_pg_typeof_Policy>; }
struct TArgs_positionjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_positionjoinsel() { return &GenericExec<TPgDirectFunc<&positionjoinsel>, true, true, TArgs_positionjoinsel_Policy>; }
struct TArgs_positionsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_positionsel() { return &GenericExec<TPgDirectFunc<&positionsel>, true, true, TArgs_positionsel_Policy>; }
struct TArgs_postgresql_fdw_validator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_postgresql_fdw_validator() { return &GenericExec<TPgDirectFunc<&postgresql_fdw_validator>, true, true, TArgs_postgresql_fdw_validator_Policy>; }
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
struct TArgs_range_gist_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_range_gist_consistent() { return &GenericExec<TPgDirectFunc<&range_gist_consistent>, true, true, TArgs_range_gist_consistent_Policy>; }
struct TArgs_range_gist_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_range_gist_penalty() { return &GenericExec<TPgDirectFunc<&range_gist_penalty>, true, true, TArgs_range_gist_penalty_Policy>; }
struct TArgs_range_gist_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_range_gist_picksplit() { return &GenericExec<TPgDirectFunc<&range_gist_picksplit>, true, true, TArgs_range_gist_picksplit_Policy>; }
struct TArgs_range_gist_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_range_gist_same() { return &GenericExec<TPgDirectFunc<&range_gist_same>, true, true, TArgs_range_gist_same_Policy>; }
struct TArgs_range_gist_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_range_gist_union() { return &GenericExec<TPgDirectFunc<&range_gist_union>, true, false, TArgs_range_gist_union_Policy>; }
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
struct TArgs_record_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_eq() { return &GenericExec<TPgDirectFunc<&record_eq>, true, true, TArgs_record_eq_Policy>; }
struct TArgs_record_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_ge() { return &GenericExec<TPgDirectFunc<&record_ge>, true, true, TArgs_record_ge_Policy>; }
struct TArgs_record_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_gt() { return &GenericExec<TPgDirectFunc<&record_gt>, true, true, TArgs_record_gt_Policy>; }
struct TArgs_record_image_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_eq() { return &GenericExec<TPgDirectFunc<&record_image_eq>, true, true, TArgs_record_image_eq_Policy>; }
struct TArgs_record_image_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_ge() { return &GenericExec<TPgDirectFunc<&record_image_ge>, true, true, TArgs_record_image_ge_Policy>; }
struct TArgs_record_image_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_gt() { return &GenericExec<TPgDirectFunc<&record_image_gt>, true, true, TArgs_record_image_gt_Policy>; }
struct TArgs_record_image_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_le() { return &GenericExec<TPgDirectFunc<&record_image_le>, true, true, TArgs_record_image_le_Policy>; }
struct TArgs_record_image_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_lt() { return &GenericExec<TPgDirectFunc<&record_image_lt>, true, true, TArgs_record_image_lt_Policy>; }
struct TArgs_record_image_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_ne() { return &GenericExec<TPgDirectFunc<&record_image_ne>, true, true, TArgs_record_image_ne_Policy>; }
struct TArgs_record_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_record_in() { return &GenericExec<TPgDirectFunc<&record_in>, true, false, TArgs_record_in_Policy>; }
struct TArgs_record_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_le() { return &GenericExec<TPgDirectFunc<&record_le>, true, true, TArgs_record_le_Policy>; }
struct TArgs_record_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_lt() { return &GenericExec<TPgDirectFunc<&record_lt>, true, true, TArgs_record_lt_Policy>; }
struct TArgs_record_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_ne() { return &GenericExec<TPgDirectFunc<&record_ne>, true, true, TArgs_record_ne_Policy>; }
struct TArgs_record_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_record_out() { return &GenericExec<TPgDirectFunc<&record_out>, true, false, TArgs_record_out_Policy>; }
struct TArgs_record_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_record_recv() { return &GenericExec<TPgDirectFunc<&record_recv>, true, false, TArgs_record_recv_Policy>; }
struct TArgs_record_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_record_send() { return &GenericExec<TPgDirectFunc<&record_send>, true, false, TArgs_record_send_Policy>; }
struct TArgs_session_user_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_session_user() { return &GenericExec<TPgDirectFunc<&session_user>, true, false, TArgs_session_user_Policy>; }
struct TArgs_setval3_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_setval3_oid() { return &GenericExec<TPgDirectFunc<&setval3_oid>, true, true, TArgs_setval3_oid_Policy>; }
struct TArgs_setval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_setval_oid() { return &GenericExec<TPgDirectFunc<&setval_oid>, true, true, TArgs_setval_oid_Policy>; }
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
struct TArgs_tsquery_rewrite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_tsquery_rewrite() { return &GenericExec<TPgDirectFunc<&tsquery_rewrite>, true, false, TArgs_tsquery_rewrite_Policy>; }
struct TArgs_tsquery_rewrite_query_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_rewrite_query() { return &GenericExec<TPgDirectFunc<&tsquery_rewrite_query>, true, false, TArgs_tsquery_rewrite_query_Policy>; }
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
