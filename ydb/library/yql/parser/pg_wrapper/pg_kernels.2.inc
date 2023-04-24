struct TArgs_PG_char_to_encoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_PG_char_to_encoding() { return &GenericExec<TPgDirectFunc<&PG_char_to_encoding>, true, true, TArgs_PG_char_to_encoding_Policy>; }
struct TArgs_PG_encoding_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_PG_encoding_to_char() { return &GenericExec<TPgDirectFunc<&PG_encoding_to_char>, true, false, TArgs_PG_encoding_to_char_Policy>; }
struct TArgs_RI_FKey_cascade_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_cascade_del() { return &GenericExec<TPgDirectFunc<&RI_FKey_cascade_del>, true, true, TArgs_RI_FKey_cascade_del_Policy>; }
struct TArgs_RI_FKey_cascade_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_cascade_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_cascade_upd>, true, true, TArgs_RI_FKey_cascade_upd_Policy>; }
struct TArgs_RI_FKey_check_ins_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_check_ins() { return &GenericExec<TPgDirectFunc<&RI_FKey_check_ins>, true, true, TArgs_RI_FKey_check_ins_Policy>; }
struct TArgs_RI_FKey_check_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_check_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_check_upd>, true, true, TArgs_RI_FKey_check_upd_Policy>; }
struct TArgs_RI_FKey_noaction_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_noaction_del() { return &GenericExec<TPgDirectFunc<&RI_FKey_noaction_del>, true, true, TArgs_RI_FKey_noaction_del_Policy>; }
struct TArgs_RI_FKey_noaction_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_noaction_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_noaction_upd>, true, true, TArgs_RI_FKey_noaction_upd_Policy>; }
struct TArgs_RI_FKey_restrict_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_restrict_del() { return &GenericExec<TPgDirectFunc<&RI_FKey_restrict_del>, true, true, TArgs_RI_FKey_restrict_del_Policy>; }
struct TArgs_RI_FKey_restrict_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_restrict_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_restrict_upd>, true, true, TArgs_RI_FKey_restrict_upd_Policy>; }
struct TArgs_RI_FKey_setdefault_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setdefault_del() { return &GenericExec<TPgDirectFunc<&RI_FKey_setdefault_del>, true, true, TArgs_RI_FKey_setdefault_del_Policy>; }
struct TArgs_RI_FKey_setdefault_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setdefault_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_setdefault_upd>, true, true, TArgs_RI_FKey_setdefault_upd_Policy>; }
struct TArgs_RI_FKey_setnull_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setnull_del() { return &GenericExec<TPgDirectFunc<&RI_FKey_setnull_del>, true, true, TArgs_RI_FKey_setnull_del_Policy>; }
struct TArgs_RI_FKey_setnull_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setnull_upd() { return &GenericExec<TPgDirectFunc<&RI_FKey_setnull_upd>, true, true, TArgs_RI_FKey_setnull_upd_Policy>; }
struct TArgs_aclcontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclcontains() { return &GenericExec<TPgDirectFunc<&aclcontains>, true, true, TArgs_aclcontains_Policy>; }
struct TArgs_acldefault_sql_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_acldefault_sql() { return &GenericExec<TPgDirectFunc<&acldefault_sql>, true, false, TArgs_acldefault_sql_Policy>; }
struct TArgs_aclinsert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclinsert() { return &GenericExec<TPgDirectFunc<&aclinsert>, true, false, TArgs_aclinsert_Policy>; }
struct TArgs_aclitem_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclitem_eq() { return &GenericExec<TPgDirectFunc<&aclitem_eq>, true, true, TArgs_aclitem_eq_Policy>; }
struct TArgs_aclitemin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_aclitemin() { return &GenericExec<TPgDirectFunc<&aclitemin>, true, false, TArgs_aclitemin_Policy>; }
struct TArgs_aclitemout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_aclitemout() { return &GenericExec<TPgDirectFunc<&aclitemout>, true, false, TArgs_aclitemout_Policy>; }
struct TArgs_aclremove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_aclremove() { return &GenericExec<TPgDirectFunc<&aclremove>, true, false, TArgs_aclremove_Policy>; }
struct TArgs_array_agg_array_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_array_agg_array_finalfn() { return &GenericExec<TPgDirectFunc<&array_agg_array_finalfn>, false, false, TArgs_array_agg_array_finalfn_Policy>; }
struct TArgs_array_agg_array_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_array_agg_array_transfn() { return &GenericExec<TPgDirectFunc<&array_agg_array_transfn>, false, true, TArgs_array_agg_array_transfn_Policy>; }
struct TArgs_array_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_array_agg_finalfn() { return &GenericExec<TPgDirectFunc<&array_agg_finalfn>, false, false, TArgs_array_agg_finalfn_Policy>; }
struct TArgs_array_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_array_agg_transfn() { return &GenericExec<TPgDirectFunc<&array_agg_transfn>, false, true, TArgs_array_agg_transfn_Policy>; }
struct TArgs_array_append_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_append() { return &GenericExec<TPgDirectFunc<&array_append>, false, false, TArgs_array_append_Policy>; }
struct TArgs_array_cardinality_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_cardinality() { return &GenericExec<TPgDirectFunc<&array_cardinality>, true, true, TArgs_array_cardinality_Policy>; }
struct TArgs_array_cat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_cat() { return &GenericExec<TPgDirectFunc<&array_cat>, false, false, TArgs_array_cat_Policy>; }
struct TArgs_array_dims_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_dims() { return &GenericExec<TPgDirectFunc<&array_dims>, true, false, TArgs_array_dims_Policy>; }
struct TArgs_array_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_eq() { return &GenericExec<TPgDirectFunc<&array_eq>, true, true, TArgs_array_eq_Policy>; }
struct TArgs_array_fill_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_array_fill() { return &GenericExec<TPgDirectFunc<&array_fill>, false, false, TArgs_array_fill_Policy>; }
struct TArgs_array_fill_with_lower_bounds_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_array_fill_with_lower_bounds() { return &GenericExec<TPgDirectFunc<&array_fill_with_lower_bounds>, false, false, TArgs_array_fill_with_lower_bounds_Policy>; }
struct TArgs_array_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_ge() { return &GenericExec<TPgDirectFunc<&array_ge>, true, true, TArgs_array_ge_Policy>; }
struct TArgs_array_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_gt() { return &GenericExec<TPgDirectFunc<&array_gt>, true, true, TArgs_array_gt_Policy>; }
struct TArgs_array_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_array_in() { return &GenericExec<TPgDirectFunc<&array_in>, true, false, TArgs_array_in_Policy>; }
struct TArgs_array_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_larger() { return &GenericExec<TPgDirectFunc<&array_larger>, true, false, TArgs_array_larger_Policy>; }
struct TArgs_array_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_le() { return &GenericExec<TPgDirectFunc<&array_le>, true, true, TArgs_array_le_Policy>; }
struct TArgs_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_length() { return &GenericExec<TPgDirectFunc<&array_length>, true, true, TArgs_array_length_Policy>; }
struct TArgs_array_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_lower() { return &GenericExec<TPgDirectFunc<&array_lower>, true, true, TArgs_array_lower_Policy>; }
struct TArgs_array_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_lt() { return &GenericExec<TPgDirectFunc<&array_lt>, true, true, TArgs_array_lt_Policy>; }
struct TArgs_array_ndims_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_ndims() { return &GenericExec<TPgDirectFunc<&array_ndims>, true, true, TArgs_array_ndims_Policy>; }
struct TArgs_array_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_ne() { return &GenericExec<TPgDirectFunc<&array_ne>, true, true, TArgs_array_ne_Policy>; }
struct TArgs_array_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_out() { return &GenericExec<TPgDirectFunc<&array_out>, true, false, TArgs_array_out_Policy>; }
struct TArgs_array_position_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_position() { return &GenericExec<TPgDirectFunc<&array_position>, false, true, TArgs_array_position_Policy>; }
struct TArgs_array_position_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_array_position_start() { return &GenericExec<TPgDirectFunc<&array_position_start>, false, true, TArgs_array_position_start_Policy>; }
struct TArgs_array_positions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_positions() { return &GenericExec<TPgDirectFunc<&array_positions>, false, false, TArgs_array_positions_Policy>; }
struct TArgs_array_prepend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_array_prepend() { return &GenericExec<TPgDirectFunc<&array_prepend>, false, false, TArgs_array_prepend_Policy>; }
struct TArgs_array_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_array_recv() { return &GenericExec<TPgDirectFunc<&array_recv>, true, false, TArgs_array_recv_Policy>; }
struct TArgs_array_remove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_remove() { return &GenericExec<TPgDirectFunc<&array_remove>, false, false, TArgs_array_remove_Policy>; }
struct TArgs_array_replace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_array_replace() { return &GenericExec<TPgDirectFunc<&array_replace>, false, false, TArgs_array_replace_Policy>; }
struct TArgs_array_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_send() { return &GenericExec<TPgDirectFunc<&array_send>, true, false, TArgs_array_send_Policy>; }
struct TArgs_array_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_smaller() { return &GenericExec<TPgDirectFunc<&array_smaller>, true, false, TArgs_array_smaller_Policy>; }
struct TArgs_array_to_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_to_json() { return &GenericExec<TPgDirectFunc<&array_to_json>, true, false, TArgs_array_to_json_Policy>; }
struct TArgs_array_to_json_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_to_json_pretty() { return &GenericExec<TPgDirectFunc<&array_to_json_pretty>, true, false, TArgs_array_to_json_pretty_Policy>; }
struct TArgs_array_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_to_tsvector() { return &GenericExec<TPgDirectFunc<&array_to_tsvector>, true, false, TArgs_array_to_tsvector_Policy>; }
struct TArgs_array_unnest_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_unnest_support() { return &GenericExec<TPgDirectFunc<&array_unnest_support>, true, true, TArgs_array_unnest_support_Policy>; }
struct TArgs_array_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_array_upper() { return &GenericExec<TPgDirectFunc<&array_upper>, true, true, TArgs_array_upper_Policy>; }
struct TArgs_arraycontained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arraycontained() { return &GenericExec<TPgDirectFunc<&arraycontained>, true, true, TArgs_arraycontained_Policy>; }
struct TArgs_arraycontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arraycontains() { return &GenericExec<TPgDirectFunc<&arraycontains>, true, true, TArgs_arraycontains_Policy>; }
struct TArgs_arrayoverlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_arrayoverlap() { return &GenericExec<TPgDirectFunc<&arrayoverlap>, true, true, TArgs_arrayoverlap_Policy>; }
struct TArgs_ascii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_ascii() { return &GenericExec<TPgDirectFunc<&ascii>, true, true, TArgs_ascii_Policy>; }
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
struct TArgs_box_above_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_above() { return &GenericExec<TPgDirectFunc<&box_above>, true, true, TArgs_box_above_Policy>; }
struct TArgs_box_above_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_above_eq() { return &GenericExec<TPgDirectFunc<&box_above_eq>, true, true, TArgs_box_above_eq_Policy>; }
struct TArgs_box_add_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_add() { return &GenericExec<TPgDirectFunc<&box_add>, true, false, TArgs_box_add_Policy>; }
struct TArgs_box_area_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_area() { return &GenericExec<TPgDirectFunc<&box_area>, true, true, TArgs_box_area_Policy>; }
struct TArgs_box_below_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_below() { return &GenericExec<TPgDirectFunc<&box_below>, true, true, TArgs_box_below_Policy>; }
struct TArgs_box_below_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_below_eq() { return &GenericExec<TPgDirectFunc<&box_below_eq>, true, true, TArgs_box_below_eq_Policy>; }
struct TArgs_box_center_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_center() { return &GenericExec<TPgDirectFunc<&box_center>, true, false, TArgs_box_center_Policy>; }
struct TArgs_box_circle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_circle() { return &GenericExec<TPgDirectFunc<&box_circle>, true, false, TArgs_box_circle_Policy>; }
struct TArgs_box_contain_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_contain() { return &GenericExec<TPgDirectFunc<&box_contain>, true, true, TArgs_box_contain_Policy>; }
struct TArgs_box_contain_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_contain_pt() { return &GenericExec<TPgDirectFunc<&box_contain_pt>, true, true, TArgs_box_contain_pt_Policy>; }
struct TArgs_box_contained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_contained() { return &GenericExec<TPgDirectFunc<&box_contained>, true, true, TArgs_box_contained_Policy>; }
struct TArgs_box_diagonal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_diagonal() { return &GenericExec<TPgDirectFunc<&box_diagonal>, true, false, TArgs_box_diagonal_Policy>; }
struct TArgs_box_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_distance() { return &GenericExec<TPgDirectFunc<&box_distance>, true, true, TArgs_box_distance_Policy>; }
struct TArgs_box_div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_div() { return &GenericExec<TPgDirectFunc<&box_div>, true, false, TArgs_box_div_Policy>; }
struct TArgs_box_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_eq() { return &GenericExec<TPgDirectFunc<&box_eq>, true, true, TArgs_box_eq_Policy>; }
struct TArgs_box_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_ge() { return &GenericExec<TPgDirectFunc<&box_ge>, true, true, TArgs_box_ge_Policy>; }
struct TArgs_box_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_gt() { return &GenericExec<TPgDirectFunc<&box_gt>, true, true, TArgs_box_gt_Policy>; }
struct TArgs_box_height_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_height() { return &GenericExec<TPgDirectFunc<&box_height>, true, true, TArgs_box_height_Policy>; }
struct TArgs_box_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_in() { return &GenericExec<TPgDirectFunc<&box_in>, true, false, TArgs_box_in_Policy>; }
struct TArgs_box_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_intersect() { return &GenericExec<TPgDirectFunc<&box_intersect>, true, false, TArgs_box_intersect_Policy>; }
struct TArgs_box_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_le() { return &GenericExec<TPgDirectFunc<&box_le>, true, true, TArgs_box_le_Policy>; }
struct TArgs_box_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_left() { return &GenericExec<TPgDirectFunc<&box_left>, true, true, TArgs_box_left_Policy>; }
struct TArgs_box_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_lt() { return &GenericExec<TPgDirectFunc<&box_lt>, true, true, TArgs_box_lt_Policy>; }
struct TArgs_box_mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_mul() { return &GenericExec<TPgDirectFunc<&box_mul>, true, false, TArgs_box_mul_Policy>; }
struct TArgs_box_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_out() { return &GenericExec<TPgDirectFunc<&box_out>, true, false, TArgs_box_out_Policy>; }
struct TArgs_box_overabove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_overabove() { return &GenericExec<TPgDirectFunc<&box_overabove>, true, true, TArgs_box_overabove_Policy>; }
struct TArgs_box_overbelow_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_overbelow() { return &GenericExec<TPgDirectFunc<&box_overbelow>, true, true, TArgs_box_overbelow_Policy>; }
struct TArgs_box_overlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_overlap() { return &GenericExec<TPgDirectFunc<&box_overlap>, true, true, TArgs_box_overlap_Policy>; }
struct TArgs_box_overleft_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_overleft() { return &GenericExec<TPgDirectFunc<&box_overleft>, true, true, TArgs_box_overleft_Policy>; }
struct TArgs_box_overright_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_overright() { return &GenericExec<TPgDirectFunc<&box_overright>, true, true, TArgs_box_overright_Policy>; }
struct TArgs_box_poly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_poly() { return &GenericExec<TPgDirectFunc<&box_poly>, true, false, TArgs_box_poly_Policy>; }
struct TArgs_box_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_box_recv() { return &GenericExec<TPgDirectFunc<&box_recv>, true, false, TArgs_box_recv_Policy>; }
struct TArgs_box_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_right() { return &GenericExec<TPgDirectFunc<&box_right>, true, true, TArgs_box_right_Policy>; }
struct TArgs_box_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_same() { return &GenericExec<TPgDirectFunc<&box_same>, true, true, TArgs_box_same_Policy>; }
struct TArgs_box_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_send() { return &GenericExec<TPgDirectFunc<&box_send>, true, false, TArgs_box_send_Policy>; }
struct TArgs_box_sub_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_box_sub() { return &GenericExec<TPgDirectFunc<&box_sub>, true, false, TArgs_box_sub_Policy>; }
struct TArgs_box_width_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_box_width() { return &GenericExec<TPgDirectFunc<&box_width>, true, true, TArgs_box_width_Policy>; }
struct TArgs_boxes_bound_box_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_boxes_bound_box() { return &GenericExec<TPgDirectFunc<&boxes_bound_box>, true, false, TArgs_boxes_bound_box_Policy>; }
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
struct TArgs_brin_desummarize_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_desummarize_range() { return &GenericExec<TPgDirectFunc<&brin_desummarize_range>, true, true, TArgs_brin_desummarize_range_Policy>; }
struct TArgs_brin_summarize_new_values_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_summarize_new_values() { return &GenericExec<TPgDirectFunc<&brin_summarize_new_values>, true, true, TArgs_brin_summarize_new_values_Policy>; }
struct TArgs_brin_summarize_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_brin_summarize_range() { return &GenericExec<TPgDirectFunc<&brin_summarize_range>, true, true, TArgs_brin_summarize_range_Policy>; }
struct TArgs_brinhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brinhandler() { return &GenericExec<TPgDirectFunc<&brinhandler>, true, true, TArgs_brinhandler_Policy>; }
struct TArgs_btarraycmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btarraycmp() { return &GenericExec<TPgDirectFunc<&btarraycmp>, true, true, TArgs_btarraycmp_Policy>; }
struct TArgs_btboolcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btboolcmp() { return &GenericExec<TPgDirectFunc<&btboolcmp>, true, true, TArgs_btboolcmp_Policy>; }
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
struct TArgs_btcharcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btcharcmp() { return &GenericExec<TPgDirectFunc<&btcharcmp>, true, true, TArgs_btcharcmp_Policy>; }
struct TArgs_btint24cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint24cmp() { return &GenericExec<TPgDirectFunc<&btint24cmp>, true, true, TArgs_btint24cmp_Policy>; }
struct TArgs_btint28cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint28cmp() { return &GenericExec<TPgDirectFunc<&btint28cmp>, true, true, TArgs_btint28cmp_Policy>; }
struct TArgs_btint2cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint2cmp() { return &GenericExec<TPgDirectFunc<&btint2cmp>, true, true, TArgs_btint2cmp_Policy>; }
struct TArgs_btint2sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btint2sortsupport() { return &GenericExec<TPgDirectFunc<&btint2sortsupport>, true, true, TArgs_btint2sortsupport_Policy>; }
struct TArgs_btint42cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint42cmp() { return &GenericExec<TPgDirectFunc<&btint42cmp>, true, true, TArgs_btint42cmp_Policy>; }
struct TArgs_btint48cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint48cmp() { return &GenericExec<TPgDirectFunc<&btint48cmp>, true, true, TArgs_btint48cmp_Policy>; }
struct TArgs_btint4cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint4cmp() { return &GenericExec<TPgDirectFunc<&btint4cmp>, true, true, TArgs_btint4cmp_Policy>; }
struct TArgs_btint4sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btint4sortsupport() { return &GenericExec<TPgDirectFunc<&btint4sortsupport>, true, true, TArgs_btint4sortsupport_Policy>; }
struct TArgs_btint82cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint82cmp() { return &GenericExec<TPgDirectFunc<&btint82cmp>, true, true, TArgs_btint82cmp_Policy>; }
struct TArgs_btint84cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint84cmp() { return &GenericExec<TPgDirectFunc<&btint84cmp>, true, true, TArgs_btint84cmp_Policy>; }
struct TArgs_btint8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btint8cmp() { return &GenericExec<TPgDirectFunc<&btint8cmp>, true, true, TArgs_btint8cmp_Policy>; }
struct TArgs_btint8sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btint8sortsupport() { return &GenericExec<TPgDirectFunc<&btint8sortsupport>, true, true, TArgs_btint8sortsupport_Policy>; }
struct TArgs_btoidcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btoidcmp() { return &GenericExec<TPgDirectFunc<&btoidcmp>, true, true, TArgs_btoidcmp_Policy>; }
struct TArgs_btoidsortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btoidsortsupport() { return &GenericExec<TPgDirectFunc<&btoidsortsupport>, true, true, TArgs_btoidsortsupport_Policy>; }
struct TArgs_btoidvectorcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btoidvectorcmp() { return &GenericExec<TPgDirectFunc<&btoidvectorcmp>, true, true, TArgs_btoidvectorcmp_Policy>; }
struct TArgs_btrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btrim() { return &GenericExec<TPgDirectFunc<&btrim>, true, false, TArgs_btrim_Policy>; }
struct TArgs_btrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_btrim1() { return &GenericExec<TPgDirectFunc<&btrim1>, true, false, TArgs_btrim1_Policy>; }
struct TArgs_bytealike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealike() { return &GenericExec<TPgDirectFunc<&bytealike>, true, true, TArgs_bytealike_Policy>; }
struct TArgs_bytealtrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealtrim() { return &GenericExec<TPgDirectFunc<&bytealtrim>, true, false, TArgs_bytealtrim_Policy>; }
struct TArgs_byteanlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteanlike() { return &GenericExec<TPgDirectFunc<&byteanlike>, true, true, TArgs_byteanlike_Policy>; }
struct TArgs_byteartrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteartrim() { return &GenericExec<TPgDirectFunc<&byteartrim>, true, false, TArgs_byteartrim_Policy>; }
struct TArgs_byteatrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteatrim() { return &GenericExec<TPgDirectFunc<&byteatrim>, true, false, TArgs_byteatrim_Policy>; }
struct TArgs_char_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_char_bpchar() { return &GenericExec<TPgDirectFunc<&char_bpchar>, true, false, TArgs_char_bpchar_Policy>; }
struct TArgs_chr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_chr() { return &GenericExec<TPgDirectFunc<&chr>, true, false, TArgs_chr_Policy>; }
struct TArgs_cidr_abbrev_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidr_abbrev() { return &GenericExec<TPgDirectFunc<&cidr_abbrev>, true, false, TArgs_cidr_abbrev_Policy>; }
struct TArgs_cidr_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidr_in() { return &GenericExec<TPgDirectFunc<&cidr_in>, true, false, TArgs_cidr_in_Policy>; }
struct TArgs_cidr_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidr_out() { return &GenericExec<TPgDirectFunc<&cidr_out>, true, false, TArgs_cidr_out_Policy>; }
struct TArgs_cidr_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidr_recv() { return &GenericExec<TPgDirectFunc<&cidr_recv>, true, false, TArgs_cidr_recv_Policy>; }
struct TArgs_cidr_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidr_send() { return &GenericExec<TPgDirectFunc<&cidr_send>, true, false, TArgs_cidr_send_Policy>; }
struct TArgs_cidr_set_masklen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_cidr_set_masklen() { return &GenericExec<TPgDirectFunc<&cidr_set_masklen>, true, false, TArgs_cidr_set_masklen_Policy>; }
struct TArgs_circle_above_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_above() { return &GenericExec<TPgDirectFunc<&circle_above>, true, true, TArgs_circle_above_Policy>; }
struct TArgs_circle_add_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_add_pt() { return &GenericExec<TPgDirectFunc<&circle_add_pt>, true, false, TArgs_circle_add_pt_Policy>; }
struct TArgs_circle_area_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_area() { return &GenericExec<TPgDirectFunc<&circle_area>, true, true, TArgs_circle_area_Policy>; }
struct TArgs_circle_below_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_below() { return &GenericExec<TPgDirectFunc<&circle_below>, true, true, TArgs_circle_below_Policy>; }
struct TArgs_circle_box_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_box() { return &GenericExec<TPgDirectFunc<&circle_box>, true, false, TArgs_circle_box_Policy>; }
struct TArgs_circle_center_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_center() { return &GenericExec<TPgDirectFunc<&circle_center>, true, false, TArgs_circle_center_Policy>; }
struct TArgs_circle_contain_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_contain() { return &GenericExec<TPgDirectFunc<&circle_contain>, true, true, TArgs_circle_contain_Policy>; }
struct TArgs_circle_contain_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_contain_pt() { return &GenericExec<TPgDirectFunc<&circle_contain_pt>, true, true, TArgs_circle_contain_pt_Policy>; }
struct TArgs_circle_contained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_contained() { return &GenericExec<TPgDirectFunc<&circle_contained>, true, true, TArgs_circle_contained_Policy>; }
struct TArgs_circle_diameter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_diameter() { return &GenericExec<TPgDirectFunc<&circle_diameter>, true, true, TArgs_circle_diameter_Policy>; }
struct TArgs_circle_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_distance() { return &GenericExec<TPgDirectFunc<&circle_distance>, true, true, TArgs_circle_distance_Policy>; }
struct TArgs_circle_div_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_div_pt() { return &GenericExec<TPgDirectFunc<&circle_div_pt>, true, false, TArgs_circle_div_pt_Policy>; }
struct TArgs_circle_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_eq() { return &GenericExec<TPgDirectFunc<&circle_eq>, true, true, TArgs_circle_eq_Policy>; }
struct TArgs_circle_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_ge() { return &GenericExec<TPgDirectFunc<&circle_ge>, true, true, TArgs_circle_ge_Policy>; }
struct TArgs_circle_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_gt() { return &GenericExec<TPgDirectFunc<&circle_gt>, true, true, TArgs_circle_gt_Policy>; }
struct TArgs_circle_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_in() { return &GenericExec<TPgDirectFunc<&circle_in>, true, false, TArgs_circle_in_Policy>; }
struct TArgs_circle_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_le() { return &GenericExec<TPgDirectFunc<&circle_le>, true, true, TArgs_circle_le_Policy>; }
struct TArgs_circle_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_left() { return &GenericExec<TPgDirectFunc<&circle_left>, true, true, TArgs_circle_left_Policy>; }
struct TArgs_circle_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_lt() { return &GenericExec<TPgDirectFunc<&circle_lt>, true, true, TArgs_circle_lt_Policy>; }
struct TArgs_circle_mul_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_mul_pt() { return &GenericExec<TPgDirectFunc<&circle_mul_pt>, true, false, TArgs_circle_mul_pt_Policy>; }
struct TArgs_circle_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_ne() { return &GenericExec<TPgDirectFunc<&circle_ne>, true, true, TArgs_circle_ne_Policy>; }
struct TArgs_circle_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_out() { return &GenericExec<TPgDirectFunc<&circle_out>, true, false, TArgs_circle_out_Policy>; }
struct TArgs_circle_overabove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_overabove() { return &GenericExec<TPgDirectFunc<&circle_overabove>, true, true, TArgs_circle_overabove_Policy>; }
struct TArgs_circle_overbelow_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_overbelow() { return &GenericExec<TPgDirectFunc<&circle_overbelow>, true, true, TArgs_circle_overbelow_Policy>; }
struct TArgs_circle_overlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_overlap() { return &GenericExec<TPgDirectFunc<&circle_overlap>, true, true, TArgs_circle_overlap_Policy>; }
struct TArgs_circle_overleft_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_overleft() { return &GenericExec<TPgDirectFunc<&circle_overleft>, true, true, TArgs_circle_overleft_Policy>; }
struct TArgs_circle_overright_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_overright() { return &GenericExec<TPgDirectFunc<&circle_overright>, true, true, TArgs_circle_overright_Policy>; }
struct TArgs_circle_poly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_circle_poly() { return &GenericExec<TPgDirectFunc<&circle_poly>, true, false, TArgs_circle_poly_Policy>; }
struct TArgs_circle_radius_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_radius() { return &GenericExec<TPgDirectFunc<&circle_radius>, true, true, TArgs_circle_radius_Policy>; }
struct TArgs_circle_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_circle_recv() { return &GenericExec<TPgDirectFunc<&circle_recv>, true, false, TArgs_circle_recv_Policy>; }
struct TArgs_circle_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_right() { return &GenericExec<TPgDirectFunc<&circle_right>, true, true, TArgs_circle_right_Policy>; }
struct TArgs_circle_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_same() { return &GenericExec<TPgDirectFunc<&circle_same>, true, true, TArgs_circle_same_Policy>; }
struct TArgs_circle_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_circle_send() { return &GenericExec<TPgDirectFunc<&circle_send>, true, false, TArgs_circle_send_Policy>; }
struct TArgs_circle_sub_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_circle_sub_pt() { return &GenericExec<TPgDirectFunc<&circle_sub_pt>, true, false, TArgs_circle_sub_pt_Policy>; }
struct TArgs_close_lb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_lb() { return &GenericExec<TPgDirectFunc<&close_lb>, true, false, TArgs_close_lb_Policy>; }
struct TArgs_close_ls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_ls() { return &GenericExec<TPgDirectFunc<&close_ls>, true, false, TArgs_close_ls_Policy>; }
struct TArgs_close_lseg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_lseg() { return &GenericExec<TPgDirectFunc<&close_lseg>, true, false, TArgs_close_lseg_Policy>; }
struct TArgs_close_pb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_pb() { return &GenericExec<TPgDirectFunc<&close_pb>, true, false, TArgs_close_pb_Policy>; }
struct TArgs_close_pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_pl() { return &GenericExec<TPgDirectFunc<&close_pl>, true, false, TArgs_close_pl_Policy>; }
struct TArgs_close_ps_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_ps() { return &GenericExec<TPgDirectFunc<&close_ps>, true, false, TArgs_close_ps_Policy>; }
struct TArgs_close_sb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_sb() { return &GenericExec<TPgDirectFunc<&close_sb>, true, false, TArgs_close_sb_Policy>; }
struct TArgs_close_sl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_close_sl() { return &GenericExec<TPgDirectFunc<&close_sl>, true, false, TArgs_close_sl_Policy>; }
struct TArgs_construct_point_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_construct_point() { return &GenericExec<TPgDirectFunc<&construct_point>, true, false, TArgs_construct_point_Policy>; }
struct TArgs_cr_circle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_cr_circle() { return &GenericExec<TPgDirectFunc<&cr_circle>, true, false, TArgs_cr_circle_Policy>; }
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
struct TArgs_dist_bl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_bl() { return &GenericExec<TPgDirectFunc<&dist_bl>, true, true, TArgs_dist_bl_Policy>; }
struct TArgs_dist_bp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_bp() { return &GenericExec<TPgDirectFunc<&dist_bp>, true, true, TArgs_dist_bp_Policy>; }
struct TArgs_dist_bs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_bs() { return &GenericExec<TPgDirectFunc<&dist_bs>, true, true, TArgs_dist_bs_Policy>; }
struct TArgs_dist_cpoint_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_cpoint() { return &GenericExec<TPgDirectFunc<&dist_cpoint>, true, true, TArgs_dist_cpoint_Policy>; }
struct TArgs_dist_cpoly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_cpoly() { return &GenericExec<TPgDirectFunc<&dist_cpoly>, true, true, TArgs_dist_cpoly_Policy>; }
struct TArgs_dist_lb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_lb() { return &GenericExec<TPgDirectFunc<&dist_lb>, true, true, TArgs_dist_lb_Policy>; }
struct TArgs_dist_lp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_lp() { return &GenericExec<TPgDirectFunc<&dist_lp>, true, true, TArgs_dist_lp_Policy>; }
struct TArgs_dist_ls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_ls() { return &GenericExec<TPgDirectFunc<&dist_ls>, true, true, TArgs_dist_ls_Policy>; }
struct TArgs_dist_pathp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_pathp() { return &GenericExec<TPgDirectFunc<&dist_pathp>, true, true, TArgs_dist_pathp_Policy>; }
struct TArgs_dist_pb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_pb() { return &GenericExec<TPgDirectFunc<&dist_pb>, true, true, TArgs_dist_pb_Policy>; }
struct TArgs_dist_pc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_pc() { return &GenericExec<TPgDirectFunc<&dist_pc>, true, true, TArgs_dist_pc_Policy>; }
struct TArgs_dist_pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_pl() { return &GenericExec<TPgDirectFunc<&dist_pl>, true, true, TArgs_dist_pl_Policy>; }
struct TArgs_dist_polyc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_polyc() { return &GenericExec<TPgDirectFunc<&dist_polyc>, true, true, TArgs_dist_polyc_Policy>; }
struct TArgs_dist_polyp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_polyp() { return &GenericExec<TPgDirectFunc<&dist_polyp>, true, true, TArgs_dist_polyp_Policy>; }
struct TArgs_dist_ppath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_ppath() { return &GenericExec<TPgDirectFunc<&dist_ppath>, true, true, TArgs_dist_ppath_Policy>; }
struct TArgs_dist_ppoly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_ppoly() { return &GenericExec<TPgDirectFunc<&dist_ppoly>, true, true, TArgs_dist_ppoly_Policy>; }
struct TArgs_dist_ps_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_ps() { return &GenericExec<TPgDirectFunc<&dist_ps>, true, true, TArgs_dist_ps_Policy>; }
struct TArgs_dist_sb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_sb() { return &GenericExec<TPgDirectFunc<&dist_sb>, true, true, TArgs_dist_sb_Policy>; }
struct TArgs_dist_sl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_sl() { return &GenericExec<TPgDirectFunc<&dist_sl>, true, true, TArgs_dist_sl_Policy>; }
struct TArgs_dist_sp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_dist_sp() { return &GenericExec<TPgDirectFunc<&dist_sp>, true, true, TArgs_dist_sp_Policy>; }
struct TArgs_dsynonym_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsynonym_init() { return &GenericExec<TPgDirectFunc<&dsynonym_init>, true, true, TArgs_dsynonym_init_Policy>; }
struct TArgs_dsynonym_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dsynonym_lexize() { return &GenericExec<TPgDirectFunc<&dsynonym_lexize>, true, true, TArgs_dsynonym_lexize_Policy>; }
struct TArgs_enum_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_cmp() { return &GenericExec<TPgDirectFunc<&enum_cmp>, true, true, TArgs_enum_cmp_Policy>; }
struct TArgs_enum_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_eq() { return &GenericExec<TPgDirectFunc<&enum_eq>, true, true, TArgs_enum_eq_Policy>; }
struct TArgs_enum_first_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_enum_first() { return &GenericExec<TPgDirectFunc<&enum_first>, false, true, TArgs_enum_first_Policy>; }
struct TArgs_enum_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_ge() { return &GenericExec<TPgDirectFunc<&enum_ge>, true, true, TArgs_enum_ge_Policy>; }
struct TArgs_enum_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_gt() { return &GenericExec<TPgDirectFunc<&enum_gt>, true, true, TArgs_enum_gt_Policy>; }
struct TArgs_enum_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_enum_in() { return &GenericExec<TPgDirectFunc<&enum_in>, true, true, TArgs_enum_in_Policy>; }
struct TArgs_enum_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_larger() { return &GenericExec<TPgDirectFunc<&enum_larger>, true, true, TArgs_enum_larger_Policy>; }
struct TArgs_enum_last_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_enum_last() { return &GenericExec<TPgDirectFunc<&enum_last>, false, true, TArgs_enum_last_Policy>; }
struct TArgs_enum_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_le() { return &GenericExec<TPgDirectFunc<&enum_le>, true, true, TArgs_enum_le_Policy>; }
struct TArgs_enum_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_lt() { return &GenericExec<TPgDirectFunc<&enum_lt>, true, true, TArgs_enum_lt_Policy>; }
struct TArgs_enum_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_ne() { return &GenericExec<TPgDirectFunc<&enum_ne>, true, true, TArgs_enum_ne_Policy>; }
struct TArgs_enum_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_enum_out() { return &GenericExec<TPgDirectFunc<&enum_out>, true, false, TArgs_enum_out_Policy>; }
struct TArgs_enum_range_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_enum_range_all() { return &GenericExec<TPgDirectFunc<&enum_range_all>, false, false, TArgs_enum_range_all_Policy>; }
struct TArgs_enum_range_bounds_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_range_bounds() { return &GenericExec<TPgDirectFunc<&enum_range_bounds>, false, false, TArgs_enum_range_bounds_Policy>; }
struct TArgs_enum_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_recv() { return &GenericExec<TPgDirectFunc<&enum_recv>, true, true, TArgs_enum_recv_Policy>; }
struct TArgs_enum_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_enum_send() { return &GenericExec<TPgDirectFunc<&enum_send>, true, false, TArgs_enum_send_Policy>; }
struct TArgs_enum_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_enum_smaller() { return &GenericExec<TPgDirectFunc<&enum_smaller>, true, true, TArgs_enum_smaller_Policy>; }
struct TArgs_eqjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_eqjoinsel() { return &GenericExec<TPgDirectFunc<&eqjoinsel>, true, true, TArgs_eqjoinsel_Policy>; }
struct TArgs_eqsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_eqsel() { return &GenericExec<TPgDirectFunc<&eqsel>, true, true, TArgs_eqsel_Policy>; }
struct TArgs_float4_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_float4_to_char() { return &GenericExec<TPgDirectFunc<&float4_to_char>, true, false, TArgs_float4_to_char_Policy>; }
struct TArgs_float8_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_float8_to_char() { return &GenericExec<TPgDirectFunc<&float8_to_char>, true, false, TArgs_float8_to_char_Policy>; }
struct TArgs_fmgr_c_validator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_fmgr_c_validator() { return &GenericExec<TPgDirectFunc<&fmgr_c_validator>, true, true, TArgs_fmgr_c_validator_Policy>; }
struct TArgs_fmgr_internal_validator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_fmgr_internal_validator() { return &GenericExec<TPgDirectFunc<&fmgr_internal_validator>, true, true, TArgs_fmgr_internal_validator_Policy>; }
struct TArgs_fmgr_sql_validator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_fmgr_sql_validator() { return &GenericExec<TPgDirectFunc<&fmgr_sql_validator>, true, true, TArgs_fmgr_sql_validator_Policy>; }
struct TArgs_gen_random_uuid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_gen_random_uuid() { return &GenericExec<TPgDirectFunc<&gen_random_uuid>, true, false, TArgs_gen_random_uuid_Policy>; }
struct TArgs_getdatabaseencoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_getdatabaseencoding() { return &GenericExec<TPgDirectFunc<&getdatabaseencoding>, true, false, TArgs_getdatabaseencoding_Policy>; }
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
struct TArgs_gist_box_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_box_consistent() { return &GenericExec<TPgDirectFunc<&gist_box_consistent>, true, true, TArgs_gist_box_consistent_Policy>; }
struct TArgs_gist_box_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_box_distance() { return &GenericExec<TPgDirectFunc<&gist_box_distance>, true, true, TArgs_gist_box_distance_Policy>; }
struct TArgs_gist_box_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gist_box_penalty() { return &GenericExec<TPgDirectFunc<&gist_box_penalty>, true, true, TArgs_gist_box_penalty_Policy>; }
struct TArgs_gist_box_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gist_box_picksplit() { return &GenericExec<TPgDirectFunc<&gist_box_picksplit>, true, true, TArgs_gist_box_picksplit_Policy>; }
struct TArgs_gist_box_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_gist_box_same() { return &GenericExec<TPgDirectFunc<&gist_box_same>, true, true, TArgs_gist_box_same_Policy>; }
struct TArgs_gist_box_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gist_box_union() { return &GenericExec<TPgDirectFunc<&gist_box_union>, true, false, TArgs_gist_box_union_Policy>; }
struct TArgs_gist_circle_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_circle_compress() { return &GenericExec<TPgDirectFunc<&gist_circle_compress>, true, true, TArgs_gist_circle_compress_Policy>; }
struct TArgs_gist_circle_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_circle_consistent() { return &GenericExec<TPgDirectFunc<&gist_circle_consistent>, true, true, TArgs_gist_circle_consistent_Policy>; }
struct TArgs_gist_circle_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_circle_distance() { return &GenericExec<TPgDirectFunc<&gist_circle_distance>, true, true, TArgs_gist_circle_distance_Policy>; }
struct TArgs_gist_point_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_compress() { return &GenericExec<TPgDirectFunc<&gist_point_compress>, true, true, TArgs_gist_point_compress_Policy>; }
struct TArgs_gist_point_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_point_consistent() { return &GenericExec<TPgDirectFunc<&gist_point_consistent>, true, true, TArgs_gist_point_consistent_Policy>; }
struct TArgs_gist_point_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_point_distance() { return &GenericExec<TPgDirectFunc<&gist_point_distance>, true, true, TArgs_gist_point_distance_Policy>; }
struct TArgs_gist_point_fetch_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_fetch() { return &GenericExec<TPgDirectFunc<&gist_point_fetch>, true, true, TArgs_gist_point_fetch_Policy>; }
struct TArgs_gist_point_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_sortsupport() { return &GenericExec<TPgDirectFunc<&gist_point_sortsupport>, true, true, TArgs_gist_point_sortsupport_Policy>; }
struct TArgs_gist_poly_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_poly_compress() { return &GenericExec<TPgDirectFunc<&gist_poly_compress>, true, true, TArgs_gist_poly_compress_Policy>; }
struct TArgs_gist_poly_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_poly_consistent() { return &GenericExec<TPgDirectFunc<&gist_poly_consistent>, true, true, TArgs_gist_poly_consistent_Policy>; }
struct TArgs_gist_poly_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_poly_distance() { return &GenericExec<TPgDirectFunc<&gist_poly_distance>, true, true, TArgs_gist_poly_distance_Policy>; }
struct TArgs_has_any_column_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_any_column_privilege_id() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_id>, true, true, TArgs_has_any_column_privilege_id_Policy>; }
struct TArgs_has_any_column_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_any_column_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_id_id>, true, true, TArgs_has_any_column_privilege_id_id_Policy>; }
struct TArgs_has_any_column_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_any_column_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_id_name>, true, true, TArgs_has_any_column_privilege_id_name_Policy>; }
struct TArgs_has_any_column_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_any_column_privilege_name() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_name>, true, true, TArgs_has_any_column_privilege_name_Policy>; }
struct TArgs_has_any_column_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_any_column_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_name_id>, true, true, TArgs_has_any_column_privilege_name_id_Policy>; }
struct TArgs_has_any_column_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_any_column_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_any_column_privilege_name_name>, true, true, TArgs_has_any_column_privilege_name_name_Policy>; }
struct TArgs_has_column_privilege_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_column_privilege_id_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_attnum>, true, true, TArgs_has_column_privilege_id_attnum_Policy>; }
struct TArgs_has_column_privilege_id_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,false};
};
TExecFunc arrow_has_column_privilege_id_id_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_id_attnum>, true, true, TArgs_has_column_privilege_id_id_attnum_Policy>; }
struct TArgs_has_column_privilege_id_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,false,false};
};
TExecFunc arrow_has_column_privilege_id_id_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_id_name>, true, true, TArgs_has_column_privilege_id_id_name_Policy>; }
struct TArgs_has_column_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_column_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_name>, true, true, TArgs_has_column_privilege_id_name_Policy>; }
struct TArgs_has_column_privilege_id_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,true,false};
};
TExecFunc arrow_has_column_privilege_id_name_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_name_attnum>, true, true, TArgs_has_column_privilege_id_name_attnum_Policy>; }
struct TArgs_has_column_privilege_id_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,false,false,false};
};
TExecFunc arrow_has_column_privilege_id_name_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_id_name_name>, true, true, TArgs_has_column_privilege_id_name_name_Policy>; }
struct TArgs_has_column_privilege_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_column_privilege_name_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_attnum>, true, true, TArgs_has_column_privilege_name_attnum_Policy>; }
struct TArgs_has_column_privilege_name_id_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,true,false};
};
TExecFunc arrow_has_column_privilege_name_id_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_id_attnum>, true, true, TArgs_has_column_privilege_name_id_attnum_Policy>; }
struct TArgs_has_column_privilege_name_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,true,false,false};
};
TExecFunc arrow_has_column_privilege_name_id_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_id_name>, true, true, TArgs_has_column_privilege_name_id_name_Policy>; }
struct TArgs_has_column_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_column_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_name>, true, true, TArgs_has_column_privilege_name_name_Policy>; }
struct TArgs_has_column_privilege_name_name_attnum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,false};
};
TExecFunc arrow_has_column_privilege_name_name_attnum() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_name_attnum>, true, true, TArgs_has_column_privilege_name_name_attnum_Policy>; }
struct TArgs_has_column_privilege_name_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
TExecFunc arrow_has_column_privilege_name_name_name() { return &GenericExec<TPgDirectFunc<&has_column_privilege_name_name_name>, true, true, TArgs_has_column_privilege_name_name_name_Policy>; }
struct TArgs_has_database_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_database_privilege_id() { return &GenericExec<TPgDirectFunc<&has_database_privilege_id>, true, true, TArgs_has_database_privilege_id_Policy>; }
struct TArgs_has_database_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_database_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_database_privilege_id_id>, true, true, TArgs_has_database_privilege_id_id_Policy>; }
struct TArgs_has_database_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_database_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_database_privilege_id_name>, true, true, TArgs_has_database_privilege_id_name_Policy>; }
struct TArgs_has_database_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_database_privilege_name() { return &GenericExec<TPgDirectFunc<&has_database_privilege_name>, true, true, TArgs_has_database_privilege_name_Policy>; }
struct TArgs_has_database_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_database_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_database_privilege_name_id>, true, true, TArgs_has_database_privilege_name_id_Policy>; }
struct TArgs_has_database_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_database_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_database_privilege_name_name>, true, true, TArgs_has_database_privilege_name_name_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_id_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_id_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_id_name_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name_id>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_id_Policy>; }
struct TArgs_has_foreign_data_wrapper_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_foreign_data_wrapper_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_foreign_data_wrapper_privilege_name_name>, true, true, TArgs_has_foreign_data_wrapper_privilege_name_name_Policy>; }
struct TArgs_has_function_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_function_privilege_id() { return &GenericExec<TPgDirectFunc<&has_function_privilege_id>, true, true, TArgs_has_function_privilege_id_Policy>; }
struct TArgs_has_function_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_function_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_function_privilege_id_id>, true, true, TArgs_has_function_privilege_id_id_Policy>; }
struct TArgs_has_function_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_function_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_function_privilege_id_name>, true, true, TArgs_has_function_privilege_id_name_Policy>; }
struct TArgs_has_function_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_function_privilege_name() { return &GenericExec<TPgDirectFunc<&has_function_privilege_name>, true, true, TArgs_has_function_privilege_name_Policy>; }
struct TArgs_has_function_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_function_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_function_privilege_name_id>, true, true, TArgs_has_function_privilege_name_id_Policy>; }
struct TArgs_has_function_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_function_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_function_privilege_name_name>, true, true, TArgs_has_function_privilege_name_name_Policy>; }
struct TArgs_has_language_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_language_privilege_id() { return &GenericExec<TPgDirectFunc<&has_language_privilege_id>, true, true, TArgs_has_language_privilege_id_Policy>; }
struct TArgs_has_language_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_language_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_language_privilege_id_id>, true, true, TArgs_has_language_privilege_id_id_Policy>; }
struct TArgs_has_language_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_language_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_language_privilege_id_name>, true, true, TArgs_has_language_privilege_id_name_Policy>; }
struct TArgs_has_language_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_language_privilege_name() { return &GenericExec<TPgDirectFunc<&has_language_privilege_name>, true, true, TArgs_has_language_privilege_name_Policy>; }
struct TArgs_has_language_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_language_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_language_privilege_name_id>, true, true, TArgs_has_language_privilege_name_id_Policy>; }
struct TArgs_has_language_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_language_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_language_privilege_name_name>, true, true, TArgs_has_language_privilege_name_name_Policy>; }
struct TArgs_has_schema_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_schema_privilege_id() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_id>, true, true, TArgs_has_schema_privilege_id_Policy>; }
struct TArgs_has_schema_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_schema_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_id_id>, true, true, TArgs_has_schema_privilege_id_id_Policy>; }
struct TArgs_has_schema_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_schema_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_id_name>, true, true, TArgs_has_schema_privilege_id_name_Policy>; }
struct TArgs_has_schema_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_schema_privilege_name() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_name>, true, true, TArgs_has_schema_privilege_name_Policy>; }
struct TArgs_has_schema_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_schema_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_name_id>, true, true, TArgs_has_schema_privilege_name_id_Policy>; }
struct TArgs_has_schema_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_schema_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_schema_privilege_name_name>, true, true, TArgs_has_schema_privilege_name_name_Policy>; }
struct TArgs_has_sequence_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_sequence_privilege_id() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_id>, true, true, TArgs_has_sequence_privilege_id_Policy>; }
struct TArgs_has_sequence_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_sequence_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_id_id>, true, true, TArgs_has_sequence_privilege_id_id_Policy>; }
struct TArgs_has_sequence_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_sequence_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_id_name>, true, true, TArgs_has_sequence_privilege_id_name_Policy>; }
struct TArgs_has_sequence_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_sequence_privilege_name() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_name>, true, true, TArgs_has_sequence_privilege_name_Policy>; }
struct TArgs_has_sequence_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_sequence_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_name_id>, true, true, TArgs_has_sequence_privilege_name_id_Policy>; }
struct TArgs_has_sequence_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_sequence_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_sequence_privilege_name_name>, true, true, TArgs_has_sequence_privilege_name_name_Policy>; }
struct TArgs_has_server_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_server_privilege_id() { return &GenericExec<TPgDirectFunc<&has_server_privilege_id>, true, true, TArgs_has_server_privilege_id_Policy>; }
struct TArgs_has_server_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_server_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_server_privilege_id_id>, true, true, TArgs_has_server_privilege_id_id_Policy>; }
struct TArgs_has_server_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_server_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_server_privilege_id_name>, true, true, TArgs_has_server_privilege_id_name_Policy>; }
struct TArgs_has_server_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_server_privilege_name() { return &GenericExec<TPgDirectFunc<&has_server_privilege_name>, true, true, TArgs_has_server_privilege_name_Policy>; }
struct TArgs_has_server_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_server_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_server_privilege_name_id>, true, true, TArgs_has_server_privilege_name_id_Policy>; }
struct TArgs_has_server_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_server_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_server_privilege_name_name>, true, true, TArgs_has_server_privilege_name_name_Policy>; }
struct TArgs_has_table_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_table_privilege_id() { return &GenericExec<TPgDirectFunc<&has_table_privilege_id>, true, true, TArgs_has_table_privilege_id_Policy>; }
struct TArgs_has_table_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_table_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_table_privilege_id_id>, true, true, TArgs_has_table_privilege_id_id_Policy>; }
struct TArgs_has_table_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_table_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_table_privilege_id_name>, true, true, TArgs_has_table_privilege_id_name_Policy>; }
struct TArgs_has_table_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_table_privilege_name() { return &GenericExec<TPgDirectFunc<&has_table_privilege_name>, true, true, TArgs_has_table_privilege_name_Policy>; }
struct TArgs_has_table_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_table_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_table_privilege_name_id>, true, true, TArgs_has_table_privilege_name_id_Policy>; }
struct TArgs_has_table_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_table_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_table_privilege_name_name>, true, true, TArgs_has_table_privilege_name_name_Policy>; }
struct TArgs_has_tablespace_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_tablespace_privilege_id() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_id>, true, true, TArgs_has_tablespace_privilege_id_Policy>; }
struct TArgs_has_tablespace_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_tablespace_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_id_id>, true, true, TArgs_has_tablespace_privilege_id_id_Policy>; }
struct TArgs_has_tablespace_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_tablespace_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_id_name>, true, true, TArgs_has_tablespace_privilege_id_name_Policy>; }
struct TArgs_has_tablespace_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_tablespace_privilege_name() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_name>, true, true, TArgs_has_tablespace_privilege_name_Policy>; }
struct TArgs_has_tablespace_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_tablespace_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_name_id>, true, true, TArgs_has_tablespace_privilege_name_id_Policy>; }
struct TArgs_has_tablespace_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_tablespace_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_tablespace_privilege_name_name>, true, true, TArgs_has_tablespace_privilege_name_name_Policy>; }
struct TArgs_has_type_privilege_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_has_type_privilege_id() { return &GenericExec<TPgDirectFunc<&has_type_privilege_id>, true, true, TArgs_has_type_privilege_id_Policy>; }
struct TArgs_has_type_privilege_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_has_type_privilege_id_id() { return &GenericExec<TPgDirectFunc<&has_type_privilege_id_id>, true, true, TArgs_has_type_privilege_id_id_Policy>; }
struct TArgs_has_type_privilege_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_has_type_privilege_id_name() { return &GenericExec<TPgDirectFunc<&has_type_privilege_id_name>, true, true, TArgs_has_type_privilege_id_name_Policy>; }
struct TArgs_has_type_privilege_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_has_type_privilege_name() { return &GenericExec<TPgDirectFunc<&has_type_privilege_name>, true, true, TArgs_has_type_privilege_name_Policy>; }
struct TArgs_has_type_privilege_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_has_type_privilege_name_id() { return &GenericExec<TPgDirectFunc<&has_type_privilege_name_id>, true, true, TArgs_has_type_privilege_name_id_Policy>; }
struct TArgs_has_type_privilege_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_has_type_privilege_name_name() { return &GenericExec<TPgDirectFunc<&has_type_privilege_name_name>, true, true, TArgs_has_type_privilege_name_name_Policy>; }
struct TArgs_hash_aclitem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_aclitem() { return &GenericExec<TPgDirectFunc<&hash_aclitem>, true, true, TArgs_hash_aclitem_Policy>; }
struct TArgs_hash_aclitem_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_aclitem_extended() { return &GenericExec<TPgDirectFunc<&hash_aclitem_extended>, true, true, TArgs_hash_aclitem_extended_Policy>; }
struct TArgs_hash_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_array() { return &GenericExec<TPgDirectFunc<&hash_array>, true, true, TArgs_hash_array_Policy>; }
struct TArgs_hash_array_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_array_extended() { return &GenericExec<TPgDirectFunc<&hash_array_extended>, true, true, TArgs_hash_array_extended_Policy>; }
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
struct TArgs_hashchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashchar() { return &GenericExec<TPgDirectFunc<&hashchar>, true, true, TArgs_hashchar_Policy>; }
struct TArgs_hashcharextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashcharextended() { return &GenericExec<TPgDirectFunc<&hashcharextended>, true, true, TArgs_hashcharextended_Policy>; }
struct TArgs_hashenum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashenum() { return &GenericExec<TPgDirectFunc<&hashenum>, true, true, TArgs_hashenum_Policy>; }
struct TArgs_hashenumextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashenumextended() { return &GenericExec<TPgDirectFunc<&hashenumextended>, true, true, TArgs_hashenumextended_Policy>; }
struct TArgs_hashfloat4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashfloat4() { return &GenericExec<TPgDirectFunc<&hashfloat4>, true, true, TArgs_hashfloat4_Policy>; }
struct TArgs_hashfloat4extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashfloat4extended() { return &GenericExec<TPgDirectFunc<&hashfloat4extended>, true, true, TArgs_hashfloat4extended_Policy>; }
struct TArgs_hashfloat8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashfloat8() { return &GenericExec<TPgDirectFunc<&hashfloat8>, true, true, TArgs_hashfloat8_Policy>; }
struct TArgs_hashfloat8extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashfloat8extended() { return &GenericExec<TPgDirectFunc<&hashfloat8extended>, true, true, TArgs_hashfloat8extended_Policy>; }
struct TArgs_hashinet_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashinet() { return &GenericExec<TPgDirectFunc<&hashinet>, true, true, TArgs_hashinet_Policy>; }
struct TArgs_hashinetextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashinetextended() { return &GenericExec<TPgDirectFunc<&hashinetextended>, true, true, TArgs_hashinetextended_Policy>; }
struct TArgs_hashint2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashint2() { return &GenericExec<TPgDirectFunc<&hashint2>, true, true, TArgs_hashint2_Policy>; }
struct TArgs_hashint2extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashint2extended() { return &GenericExec<TPgDirectFunc<&hashint2extended>, true, true, TArgs_hashint2extended_Policy>; }
struct TArgs_hashint4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashint4() { return &GenericExec<TPgDirectFunc<&hashint4>, true, true, TArgs_hashint4_Policy>; }
struct TArgs_hashint4extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashint4extended() { return &GenericExec<TPgDirectFunc<&hashint4extended>, true, true, TArgs_hashint4extended_Policy>; }
struct TArgs_hashint8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashint8() { return &GenericExec<TPgDirectFunc<&hashint8>, true, true, TArgs_hashint8_Policy>; }
struct TArgs_hashint8extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashint8extended() { return &GenericExec<TPgDirectFunc<&hashint8extended>, true, true, TArgs_hashint8extended_Policy>; }
struct TArgs_hashname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashname() { return &GenericExec<TPgDirectFunc<&hashname>, true, true, TArgs_hashname_Policy>; }
struct TArgs_hashnameextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashnameextended() { return &GenericExec<TPgDirectFunc<&hashnameextended>, true, true, TArgs_hashnameextended_Policy>; }
struct TArgs_hashoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashoid() { return &GenericExec<TPgDirectFunc<&hashoid>, true, true, TArgs_hashoid_Policy>; }
struct TArgs_hashoidextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashoidextended() { return &GenericExec<TPgDirectFunc<&hashoidextended>, true, true, TArgs_hashoidextended_Policy>; }
struct TArgs_hashoidvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashoidvector() { return &GenericExec<TPgDirectFunc<&hashoidvector>, true, true, TArgs_hashoidvector_Policy>; }
struct TArgs_hashoidvectorextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashoidvectorextended() { return &GenericExec<TPgDirectFunc<&hashoidvectorextended>, true, true, TArgs_hashoidvectorextended_Policy>; }
struct TArgs_hashtext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashtext() { return &GenericExec<TPgDirectFunc<&hashtext>, true, true, TArgs_hashtext_Policy>; }
struct TArgs_hashtextextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashtextextended() { return &GenericExec<TPgDirectFunc<&hashtextextended>, true, true, TArgs_hashtextextended_Policy>; }
struct TArgs_hashvarlena_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashvarlena() { return &GenericExec<TPgDirectFunc<&hashvarlena>, true, true, TArgs_hashvarlena_Policy>; }
struct TArgs_hashvarlenaextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_hashvarlenaextended() { return &GenericExec<TPgDirectFunc<&hashvarlenaextended>, true, true, TArgs_hashvarlenaextended_Policy>; }
struct TArgs_heap_tableam_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_heap_tableam_handler() { return &GenericExec<TPgDirectFunc<&heap_tableam_handler>, true, true, TArgs_heap_tableam_handler_Policy>; }
struct TArgs_inet_abbrev_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inet_abbrev() { return &GenericExec<TPgDirectFunc<&inet_abbrev>, true, false, TArgs_inet_abbrev_Policy>; }
struct TArgs_inet_client_addr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_inet_client_addr() { return &GenericExec<TPgDirectFunc<&inet_client_addr>, false, false, TArgs_inet_client_addr_Policy>; }
struct TArgs_inet_client_port_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_inet_client_port() { return &GenericExec<TPgDirectFunc<&inet_client_port>, false, true, TArgs_inet_client_port_Policy>; }
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
struct TArgs_inet_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inet_in() { return &GenericExec<TPgDirectFunc<&inet_in>, true, false, TArgs_inet_in_Policy>; }
struct TArgs_inet_merge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inet_merge() { return &GenericExec<TPgDirectFunc<&inet_merge>, true, false, TArgs_inet_merge_Policy>; }
struct TArgs_inet_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inet_out() { return &GenericExec<TPgDirectFunc<&inet_out>, true, false, TArgs_inet_out_Policy>; }
struct TArgs_inet_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_inet_recv() { return &GenericExec<TPgDirectFunc<&inet_recv>, true, false, TArgs_inet_recv_Policy>; }
struct TArgs_inet_same_family_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inet_same_family() { return &GenericExec<TPgDirectFunc<&inet_same_family>, true, true, TArgs_inet_same_family_Policy>; }
struct TArgs_inet_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inet_send() { return &GenericExec<TPgDirectFunc<&inet_send>, true, false, TArgs_inet_send_Policy>; }
struct TArgs_inet_server_addr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_inet_server_addr() { return &GenericExec<TPgDirectFunc<&inet_server_addr>, false, false, TArgs_inet_server_addr_Policy>; }
struct TArgs_inet_server_port_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_inet_server_port() { return &GenericExec<TPgDirectFunc<&inet_server_port>, false, true, TArgs_inet_server_port_Policy>; }
struct TArgs_inet_set_masklen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_inet_set_masklen() { return &GenericExec<TPgDirectFunc<&inet_set_masklen>, true, false, TArgs_inet_set_masklen_Policy>; }
struct TArgs_inet_spg_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_choose() { return &GenericExec<TPgDirectFunc<&inet_spg_choose>, true, true, TArgs_inet_spg_choose_Policy>; }
struct TArgs_inet_spg_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_config() { return &GenericExec<TPgDirectFunc<&inet_spg_config>, true, true, TArgs_inet_spg_config_Policy>; }
struct TArgs_inet_spg_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_inner_consistent() { return &GenericExec<TPgDirectFunc<&inet_spg_inner_consistent>, true, true, TArgs_inet_spg_inner_consistent_Policy>; }
struct TArgs_inet_spg_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_leaf_consistent() { return &GenericExec<TPgDirectFunc<&inet_spg_leaf_consistent>, true, true, TArgs_inet_spg_leaf_consistent_Policy>; }
struct TArgs_inet_spg_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_inet_spg_picksplit() { return &GenericExec<TPgDirectFunc<&inet_spg_picksplit>, true, true, TArgs_inet_spg_picksplit_Policy>; }
struct TArgs_inet_to_cidr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inet_to_cidr() { return &GenericExec<TPgDirectFunc<&inet_to_cidr>, true, false, TArgs_inet_to_cidr_Policy>; }
struct TArgs_inetand_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inetand() { return &GenericExec<TPgDirectFunc<&inetand>, true, false, TArgs_inetand_Policy>; }
struct TArgs_inetmi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inetmi() { return &GenericExec<TPgDirectFunc<&inetmi>, true, true, TArgs_inetmi_Policy>; }
struct TArgs_inetmi_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_inetmi_int8() { return &GenericExec<TPgDirectFunc<&inetmi_int8>, true, false, TArgs_inetmi_int8_Policy>; }
struct TArgs_inetnot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_inetnot() { return &GenericExec<TPgDirectFunc<&inetnot>, true, false, TArgs_inetnot_Policy>; }
struct TArgs_inetor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inetor() { return &GenericExec<TPgDirectFunc<&inetor>, true, false, TArgs_inetor_Policy>; }
struct TArgs_inetpl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_inetpl() { return &GenericExec<TPgDirectFunc<&inetpl>, true, false, TArgs_inetpl_Policy>; }
struct TArgs_initcap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_initcap() { return &GenericExec<TPgDirectFunc<&initcap>, true, false, TArgs_initcap_Policy>; }
struct TArgs_int4_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_int4_to_char() { return &GenericExec<TPgDirectFunc<&int4_to_char>, true, false, TArgs_int4_to_char_Policy>; }
struct TArgs_int8_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_int8_to_char() { return &GenericExec<TPgDirectFunc<&int8_to_char>, true, false, TArgs_int8_to_char_Policy>; }
struct TArgs_inter_lb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inter_lb() { return &GenericExec<TPgDirectFunc<&inter_lb>, true, true, TArgs_inter_lb_Policy>; }
struct TArgs_inter_sb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inter_sb() { return &GenericExec<TPgDirectFunc<&inter_sb>, true, true, TArgs_inter_sb_Policy>; }
struct TArgs_inter_sl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_inter_sl() { return &GenericExec<TPgDirectFunc<&inter_sl>, true, true, TArgs_inter_sl_Policy>; }
struct TArgs_interval_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_to_char() { return &GenericExec<TPgDirectFunc<&interval_to_char>, true, false, TArgs_interval_to_char_Policy>; }
struct TArgs_json_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_json_agg_finalfn() { return &GenericExec<TPgDirectFunc<&json_agg_finalfn>, false, false, TArgs_json_agg_finalfn_Policy>; }
struct TArgs_json_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_json_agg_transfn() { return &GenericExec<TPgDirectFunc<&json_agg_transfn>, false, true, TArgs_json_agg_transfn_Policy>; }
struct TArgs_json_build_array_noargs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_json_build_array_noargs() { return &GenericExec<TPgDirectFunc<&json_build_array_noargs>, false, false, TArgs_json_build_array_noargs_Policy>; }
struct TArgs_json_build_object_noargs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_json_build_object_noargs() { return &GenericExec<TPgDirectFunc<&json_build_object_noargs>, false, false, TArgs_json_build_object_noargs_Policy>; }
struct TArgs_json_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_in() { return &GenericExec<TPgDirectFunc<&json_in>, true, false, TArgs_json_in_Policy>; }
struct TArgs_json_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_object() { return &GenericExec<TPgDirectFunc<&json_object>, true, false, TArgs_json_object_Policy>; }
struct TArgs_json_object_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_json_object_agg_finalfn() { return &GenericExec<TPgDirectFunc<&json_object_agg_finalfn>, false, false, TArgs_json_object_agg_finalfn_Policy>; }
struct TArgs_json_object_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_json_object_agg_transfn() { return &GenericExec<TPgDirectFunc<&json_object_agg_transfn>, false, true, TArgs_json_object_agg_transfn_Policy>; }
struct TArgs_json_object_two_arg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_json_object_two_arg() { return &GenericExec<TPgDirectFunc<&json_object_two_arg>, true, false, TArgs_json_object_two_arg_Policy>; }
struct TArgs_json_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_out() { return &GenericExec<TPgDirectFunc<&json_out>, true, false, TArgs_json_out_Policy>; }
struct TArgs_json_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_json_recv() { return &GenericExec<TPgDirectFunc<&json_recv>, true, false, TArgs_json_recv_Policy>; }
struct TArgs_json_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_send() { return &GenericExec<TPgDirectFunc<&json_send>, true, false, TArgs_json_send_Policy>; }
struct TArgs_json_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_typeof() { return &GenericExec<TPgDirectFunc<&json_typeof>, true, false, TArgs_json_typeof_Policy>; }
struct TArgs_jsonb_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonb_agg_finalfn() { return &GenericExec<TPgDirectFunc<&jsonb_agg_finalfn>, false, false, TArgs_jsonb_agg_finalfn_Policy>; }
struct TArgs_jsonb_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_jsonb_agg_transfn() { return &GenericExec<TPgDirectFunc<&jsonb_agg_transfn>, false, true, TArgs_jsonb_agg_transfn_Policy>; }
struct TArgs_jsonb_bool_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_bool() { return &GenericExec<TPgDirectFunc<&jsonb_bool>, true, true, TArgs_jsonb_bool_Policy>; }
struct TArgs_jsonb_build_array_noargs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_jsonb_build_array_noargs() { return &GenericExec<TPgDirectFunc<&jsonb_build_array_noargs>, false, false, TArgs_jsonb_build_array_noargs_Policy>; }
struct TArgs_jsonb_build_object_noargs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_jsonb_build_object_noargs() { return &GenericExec<TPgDirectFunc<&jsonb_build_object_noargs>, false, false, TArgs_jsonb_build_object_noargs_Policy>; }
struct TArgs_jsonb_float4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_float4() { return &GenericExec<TPgDirectFunc<&jsonb_float4>, true, true, TArgs_jsonb_float4_Policy>; }
struct TArgs_jsonb_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_float8() { return &GenericExec<TPgDirectFunc<&jsonb_float8>, true, true, TArgs_jsonb_float8_Policy>; }
struct TArgs_jsonb_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_in() { return &GenericExec<TPgDirectFunc<&jsonb_in>, true, false, TArgs_jsonb_in_Policy>; }
struct TArgs_jsonb_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_int2() { return &GenericExec<TPgDirectFunc<&jsonb_int2>, true, true, TArgs_jsonb_int2_Policy>; }
struct TArgs_jsonb_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_int4() { return &GenericExec<TPgDirectFunc<&jsonb_int4>, true, true, TArgs_jsonb_int4_Policy>; }
struct TArgs_jsonb_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_int8() { return &GenericExec<TPgDirectFunc<&jsonb_int8>, true, true, TArgs_jsonb_int8_Policy>; }
struct TArgs_jsonb_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_numeric() { return &GenericExec<TPgDirectFunc<&jsonb_numeric>, true, false, TArgs_jsonb_numeric_Policy>; }
struct TArgs_jsonb_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_object() { return &GenericExec<TPgDirectFunc<&jsonb_object>, true, false, TArgs_jsonb_object_Policy>; }
struct TArgs_jsonb_object_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonb_object_agg_finalfn() { return &GenericExec<TPgDirectFunc<&jsonb_object_agg_finalfn>, false, false, TArgs_jsonb_object_agg_finalfn_Policy>; }
struct TArgs_jsonb_object_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_jsonb_object_agg_transfn() { return &GenericExec<TPgDirectFunc<&jsonb_object_agg_transfn>, false, true, TArgs_jsonb_object_agg_transfn_Policy>; }
struct TArgs_jsonb_object_two_arg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_object_two_arg() { return &GenericExec<TPgDirectFunc<&jsonb_object_two_arg>, true, false, TArgs_jsonb_object_two_arg_Policy>; }
struct TArgs_jsonb_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_out() { return &GenericExec<TPgDirectFunc<&jsonb_out>, true, false, TArgs_jsonb_out_Policy>; }
struct TArgs_jsonb_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonb_recv() { return &GenericExec<TPgDirectFunc<&jsonb_recv>, true, false, TArgs_jsonb_recv_Policy>; }
struct TArgs_jsonb_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_send() { return &GenericExec<TPgDirectFunc<&jsonb_send>, true, false, TArgs_jsonb_send_Policy>; }
struct TArgs_jsonb_subscript_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_jsonb_subscript_handler() { return &GenericExec<TPgDirectFunc<&jsonb_subscript_handler>, true, true, TArgs_jsonb_subscript_handler_Policy>; }
struct TArgs_jsonb_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_typeof() { return &GenericExec<TPgDirectFunc<&jsonb_typeof>, true, false, TArgs_jsonb_typeof_Policy>; }
struct TArgs_length_in_encoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_length_in_encoding() { return &GenericExec<TPgDirectFunc<&length_in_encoding>, true, true, TArgs_length_in_encoding_Policy>; }
struct TArgs_like_escape_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_like_escape() { return &GenericExec<TPgDirectFunc<&like_escape>, true, false, TArgs_like_escape_Policy>; }
struct TArgs_like_escape_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_like_escape_bytea() { return &GenericExec<TPgDirectFunc<&like_escape_bytea>, true, false, TArgs_like_escape_bytea_Policy>; }
struct TArgs_line_construct_pp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_construct_pp() { return &GenericExec<TPgDirectFunc<&line_construct_pp>, true, false, TArgs_line_construct_pp_Policy>; }
struct TArgs_line_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_distance() { return &GenericExec<TPgDirectFunc<&line_distance>, true, true, TArgs_line_distance_Policy>; }
struct TArgs_line_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_eq() { return &GenericExec<TPgDirectFunc<&line_eq>, true, true, TArgs_line_eq_Policy>; }
struct TArgs_line_horizontal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_line_horizontal() { return &GenericExec<TPgDirectFunc<&line_horizontal>, true, true, TArgs_line_horizontal_Policy>; }
struct TArgs_line_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_line_in() { return &GenericExec<TPgDirectFunc<&line_in>, true, false, TArgs_line_in_Policy>; }
struct TArgs_line_interpt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_interpt() { return &GenericExec<TPgDirectFunc<&line_interpt>, true, false, TArgs_line_interpt_Policy>; }
struct TArgs_line_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_intersect() { return &GenericExec<TPgDirectFunc<&line_intersect>, true, true, TArgs_line_intersect_Policy>; }
struct TArgs_line_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_line_out() { return &GenericExec<TPgDirectFunc<&line_out>, true, false, TArgs_line_out_Policy>; }
struct TArgs_line_parallel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_parallel() { return &GenericExec<TPgDirectFunc<&line_parallel>, true, true, TArgs_line_parallel_Policy>; }
struct TArgs_line_perp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_line_perp() { return &GenericExec<TPgDirectFunc<&line_perp>, true, true, TArgs_line_perp_Policy>; }
struct TArgs_line_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_line_recv() { return &GenericExec<TPgDirectFunc<&line_recv>, true, false, TArgs_line_recv_Policy>; }
struct TArgs_line_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_line_send() { return &GenericExec<TPgDirectFunc<&line_send>, true, false, TArgs_line_send_Policy>; }
struct TArgs_line_vertical_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_line_vertical() { return &GenericExec<TPgDirectFunc<&line_vertical>, true, true, TArgs_line_vertical_Policy>; }
struct TArgs_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lower() { return &GenericExec<TPgDirectFunc<&lower>, true, false, TArgs_lower_Policy>; }
struct TArgs_lpad_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_lpad() { return &GenericExec<TPgDirectFunc<&lpad>, true, false, TArgs_lpad_Policy>; }
struct TArgs_lseg_center_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_center() { return &GenericExec<TPgDirectFunc<&lseg_center>, true, false, TArgs_lseg_center_Policy>; }
struct TArgs_lseg_construct_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_construct() { return &GenericExec<TPgDirectFunc<&lseg_construct>, true, false, TArgs_lseg_construct_Policy>; }
struct TArgs_lseg_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_distance() { return &GenericExec<TPgDirectFunc<&lseg_distance>, true, true, TArgs_lseg_distance_Policy>; }
struct TArgs_lseg_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_eq() { return &GenericExec<TPgDirectFunc<&lseg_eq>, true, true, TArgs_lseg_eq_Policy>; }
struct TArgs_lseg_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_ge() { return &GenericExec<TPgDirectFunc<&lseg_ge>, true, true, TArgs_lseg_ge_Policy>; }
struct TArgs_lseg_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_gt() { return &GenericExec<TPgDirectFunc<&lseg_gt>, true, true, TArgs_lseg_gt_Policy>; }
struct TArgs_lseg_horizontal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_horizontal() { return &GenericExec<TPgDirectFunc<&lseg_horizontal>, true, true, TArgs_lseg_horizontal_Policy>; }
struct TArgs_lseg_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_in() { return &GenericExec<TPgDirectFunc<&lseg_in>, true, false, TArgs_lseg_in_Policy>; }
struct TArgs_lseg_interpt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_interpt() { return &GenericExec<TPgDirectFunc<&lseg_interpt>, true, false, TArgs_lseg_interpt_Policy>; }
struct TArgs_lseg_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_intersect() { return &GenericExec<TPgDirectFunc<&lseg_intersect>, true, true, TArgs_lseg_intersect_Policy>; }
struct TArgs_lseg_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_le() { return &GenericExec<TPgDirectFunc<&lseg_le>, true, true, TArgs_lseg_le_Policy>; }
struct TArgs_lseg_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_length() { return &GenericExec<TPgDirectFunc<&lseg_length>, true, true, TArgs_lseg_length_Policy>; }
struct TArgs_lseg_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_lt() { return &GenericExec<TPgDirectFunc<&lseg_lt>, true, true, TArgs_lseg_lt_Policy>; }
struct TArgs_lseg_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_ne() { return &GenericExec<TPgDirectFunc<&lseg_ne>, true, true, TArgs_lseg_ne_Policy>; }
struct TArgs_lseg_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_out() { return &GenericExec<TPgDirectFunc<&lseg_out>, true, false, TArgs_lseg_out_Policy>; }
struct TArgs_lseg_parallel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_parallel() { return &GenericExec<TPgDirectFunc<&lseg_parallel>, true, true, TArgs_lseg_parallel_Policy>; }
struct TArgs_lseg_perp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_lseg_perp() { return &GenericExec<TPgDirectFunc<&lseg_perp>, true, true, TArgs_lseg_perp_Policy>; }
struct TArgs_lseg_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_lseg_recv() { return &GenericExec<TPgDirectFunc<&lseg_recv>, true, false, TArgs_lseg_recv_Policy>; }
struct TArgs_lseg_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_send() { return &GenericExec<TPgDirectFunc<&lseg_send>, true, false, TArgs_lseg_send_Policy>; }
struct TArgs_lseg_vertical_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_lseg_vertical() { return &GenericExec<TPgDirectFunc<&lseg_vertical>, true, true, TArgs_lseg_vertical_Policy>; }
struct TArgs_ltrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ltrim() { return &GenericExec<TPgDirectFunc<&ltrim>, true, false, TArgs_ltrim_Policy>; }
struct TArgs_ltrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_ltrim1() { return &GenericExec<TPgDirectFunc<&ltrim1>, true, false, TArgs_ltrim1_Policy>; }
struct TArgs_makeaclitem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,false,true};
};
TExecFunc arrow_makeaclitem() { return &GenericExec<TPgDirectFunc<&makeaclitem>, true, false, TArgs_makeaclitem_Policy>; }
struct TArgs_matchingjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_matchingjoinsel() { return &GenericExec<TPgDirectFunc<&matchingjoinsel>, true, true, TArgs_matchingjoinsel_Policy>; }
struct TArgs_matchingsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_matchingsel() { return &GenericExec<TPgDirectFunc<&matchingsel>, true, true, TArgs_matchingsel_Policy>; }
struct TArgs_md5_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_md5_bytea() { return &GenericExec<TPgDirectFunc<&md5_bytea>, true, false, TArgs_md5_bytea_Policy>; }
struct TArgs_md5_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_md5_text() { return &GenericExec<TPgDirectFunc<&md5_text>, true, false, TArgs_md5_text_Policy>; }
struct TArgs_multirange_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_multirange_typanalyze() { return &GenericExec<TPgDirectFunc<&multirange_typanalyze>, true, true, TArgs_multirange_typanalyze_Policy>; }
struct TArgs_name_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_name_bpchar() { return &GenericExec<TPgDirectFunc<&name_bpchar>, true, false, TArgs_name_bpchar_Policy>; }
struct TArgs_nameiclike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameiclike() { return &GenericExec<TPgDirectFunc<&nameiclike>, true, true, TArgs_nameiclike_Policy>; }
struct TArgs_nameicnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameicnlike() { return &GenericExec<TPgDirectFunc<&nameicnlike>, true, true, TArgs_nameicnlike_Policy>; }
struct TArgs_namelike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelike() { return &GenericExec<TPgDirectFunc<&namelike>, true, true, TArgs_namelike_Policy>; }
struct TArgs_namenlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namenlike() { return &GenericExec<TPgDirectFunc<&namenlike>, true, true, TArgs_namenlike_Policy>; }
struct TArgs_neqjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_neqjoinsel() { return &GenericExec<TPgDirectFunc<&neqjoinsel>, true, true, TArgs_neqjoinsel_Policy>; }
struct TArgs_neqsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_neqsel() { return &GenericExec<TPgDirectFunc<&neqsel>, true, true, TArgs_neqsel_Policy>; }
struct TArgs_network_broadcast_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_broadcast() { return &GenericExec<TPgDirectFunc<&network_broadcast>, true, false, TArgs_network_broadcast_Policy>; }
struct TArgs_network_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_cmp() { return &GenericExec<TPgDirectFunc<&network_cmp>, true, true, TArgs_network_cmp_Policy>; }
struct TArgs_network_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_eq() { return &GenericExec<TPgDirectFunc<&network_eq>, true, true, TArgs_network_eq_Policy>; }
struct TArgs_network_family_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_family() { return &GenericExec<TPgDirectFunc<&network_family>, true, true, TArgs_network_family_Policy>; }
struct TArgs_network_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_ge() { return &GenericExec<TPgDirectFunc<&network_ge>, true, true, TArgs_network_ge_Policy>; }
struct TArgs_network_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_gt() { return &GenericExec<TPgDirectFunc<&network_gt>, true, true, TArgs_network_gt_Policy>; }
struct TArgs_network_host_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_host() { return &GenericExec<TPgDirectFunc<&network_host>, true, false, TArgs_network_host_Policy>; }
struct TArgs_network_hostmask_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_hostmask() { return &GenericExec<TPgDirectFunc<&network_hostmask>, true, false, TArgs_network_hostmask_Policy>; }
struct TArgs_network_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_larger() { return &GenericExec<TPgDirectFunc<&network_larger>, true, false, TArgs_network_larger_Policy>; }
struct TArgs_network_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_le() { return &GenericExec<TPgDirectFunc<&network_le>, true, true, TArgs_network_le_Policy>; }
struct TArgs_network_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_lt() { return &GenericExec<TPgDirectFunc<&network_lt>, true, true, TArgs_network_lt_Policy>; }
struct TArgs_network_masklen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_masklen() { return &GenericExec<TPgDirectFunc<&network_masklen>, true, true, TArgs_network_masklen_Policy>; }
struct TArgs_network_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_ne() { return &GenericExec<TPgDirectFunc<&network_ne>, true, true, TArgs_network_ne_Policy>; }
struct TArgs_network_netmask_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_netmask() { return &GenericExec<TPgDirectFunc<&network_netmask>, true, false, TArgs_network_netmask_Policy>; }
struct TArgs_network_network_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_network() { return &GenericExec<TPgDirectFunc<&network_network>, true, false, TArgs_network_network_Policy>; }
struct TArgs_network_overlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_overlap() { return &GenericExec<TPgDirectFunc<&network_overlap>, true, true, TArgs_network_overlap_Policy>; }
struct TArgs_network_show_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_network_show() { return &GenericExec<TPgDirectFunc<&network_show>, true, false, TArgs_network_show_Policy>; }
struct TArgs_network_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_smaller() { return &GenericExec<TPgDirectFunc<&network_smaller>, true, false, TArgs_network_smaller_Policy>; }
struct TArgs_network_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_network_sortsupport() { return &GenericExec<TPgDirectFunc<&network_sortsupport>, true, true, TArgs_network_sortsupport_Policy>; }
struct TArgs_network_sub_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_sub() { return &GenericExec<TPgDirectFunc<&network_sub>, true, true, TArgs_network_sub_Policy>; }
struct TArgs_network_subeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_subeq() { return &GenericExec<TPgDirectFunc<&network_subeq>, true, true, TArgs_network_subeq_Policy>; }
struct TArgs_network_subset_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_network_subset_support() { return &GenericExec<TPgDirectFunc<&network_subset_support>, true, true, TArgs_network_subset_support_Policy>; }
struct TArgs_network_sup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_sup() { return &GenericExec<TPgDirectFunc<&network_sup>, true, true, TArgs_network_sup_Policy>; }
struct TArgs_network_supeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_network_supeq() { return &GenericExec<TPgDirectFunc<&network_supeq>, true, true, TArgs_network_supeq_Policy>; }
struct TArgs_numeric_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_to_char() { return &GenericExec<TPgDirectFunc<&numeric_to_char>, true, false, TArgs_numeric_to_char_Policy>; }
struct TArgs_numeric_to_number_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_to_number() { return &GenericExec<TPgDirectFunc<&numeric_to_number>, true, false, TArgs_numeric_to_number_Policy>; }
struct TArgs_on_pb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_pb() { return &GenericExec<TPgDirectFunc<&on_pb>, true, true, TArgs_on_pb_Policy>; }
struct TArgs_on_pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_pl() { return &GenericExec<TPgDirectFunc<&on_pl>, true, true, TArgs_on_pl_Policy>; }
struct TArgs_on_ppath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_ppath() { return &GenericExec<TPgDirectFunc<&on_ppath>, true, true, TArgs_on_ppath_Policy>; }
struct TArgs_on_ps_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_ps() { return &GenericExec<TPgDirectFunc<&on_ps>, true, true, TArgs_on_ps_Policy>; }
struct TArgs_on_sb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_sb() { return &GenericExec<TPgDirectFunc<&on_sb>, true, true, TArgs_on_sb_Policy>; }
struct TArgs_on_sl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_on_sl() { return &GenericExec<TPgDirectFunc<&on_sl>, true, true, TArgs_on_sl_Policy>; }
struct TArgs_path_add_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_add() { return &GenericExec<TPgDirectFunc<&path_add>, true, false, TArgs_path_add_Policy>; }
struct TArgs_path_add_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_add_pt() { return &GenericExec<TPgDirectFunc<&path_add_pt>, true, false, TArgs_path_add_pt_Policy>; }
struct TArgs_path_area_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_area() { return &GenericExec<TPgDirectFunc<&path_area>, true, true, TArgs_path_area_Policy>; }
struct TArgs_path_center_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_center() { return &GenericExec<TPgDirectFunc<&path_center>, true, false, TArgs_path_center_Policy>; }
struct TArgs_path_close_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_close() { return &GenericExec<TPgDirectFunc<&path_close>, true, false, TArgs_path_close_Policy>; }
struct TArgs_path_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_distance() { return &GenericExec<TPgDirectFunc<&path_distance>, true, true, TArgs_path_distance_Policy>; }
struct TArgs_path_div_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_div_pt() { return &GenericExec<TPgDirectFunc<&path_div_pt>, true, false, TArgs_path_div_pt_Policy>; }
struct TArgs_path_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_in() { return &GenericExec<TPgDirectFunc<&path_in>, true, false, TArgs_path_in_Policy>; }
struct TArgs_path_inter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_inter() { return &GenericExec<TPgDirectFunc<&path_inter>, true, true, TArgs_path_inter_Policy>; }
struct TArgs_path_isclosed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_isclosed() { return &GenericExec<TPgDirectFunc<&path_isclosed>, true, true, TArgs_path_isclosed_Policy>; }
struct TArgs_path_isopen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_isopen() { return &GenericExec<TPgDirectFunc<&path_isopen>, true, true, TArgs_path_isopen_Policy>; }
struct TArgs_path_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_length() { return &GenericExec<TPgDirectFunc<&path_length>, true, true, TArgs_path_length_Policy>; }
struct TArgs_path_mul_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_mul_pt() { return &GenericExec<TPgDirectFunc<&path_mul_pt>, true, false, TArgs_path_mul_pt_Policy>; }
struct TArgs_path_n_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_n_eq() { return &GenericExec<TPgDirectFunc<&path_n_eq>, true, true, TArgs_path_n_eq_Policy>; }
struct TArgs_path_n_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_n_ge() { return &GenericExec<TPgDirectFunc<&path_n_ge>, true, true, TArgs_path_n_ge_Policy>; }
struct TArgs_path_n_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_n_gt() { return &GenericExec<TPgDirectFunc<&path_n_gt>, true, true, TArgs_path_n_gt_Policy>; }
struct TArgs_path_n_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_n_le() { return &GenericExec<TPgDirectFunc<&path_n_le>, true, true, TArgs_path_n_le_Policy>; }
struct TArgs_path_n_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_n_lt() { return &GenericExec<TPgDirectFunc<&path_n_lt>, true, true, TArgs_path_n_lt_Policy>; }
struct TArgs_path_npoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_npoints() { return &GenericExec<TPgDirectFunc<&path_npoints>, true, true, TArgs_path_npoints_Policy>; }
struct TArgs_path_open_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_open() { return &GenericExec<TPgDirectFunc<&path_open>, true, false, TArgs_path_open_Policy>; }
struct TArgs_path_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_out() { return &GenericExec<TPgDirectFunc<&path_out>, true, false, TArgs_path_out_Policy>; }
struct TArgs_path_poly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_poly() { return &GenericExec<TPgDirectFunc<&path_poly>, true, false, TArgs_path_poly_Policy>; }
struct TArgs_path_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_path_recv() { return &GenericExec<TPgDirectFunc<&path_recv>, true, false, TArgs_path_recv_Policy>; }
struct TArgs_path_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_path_send() { return &GenericExec<TPgDirectFunc<&path_send>, true, false, TArgs_path_send_Policy>; }
struct TArgs_path_sub_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_path_sub_pt() { return &GenericExec<TPgDirectFunc<&path_sub_pt>, true, false, TArgs_path_sub_pt_Policy>; }
struct TArgs_pg_client_encoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_client_encoding() { return &GenericExec<TPgDirectFunc<&pg_client_encoding>, true, false, TArgs_pg_client_encoding_Policy>; }
struct TArgs_pg_convert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_pg_convert() { return &GenericExec<TPgDirectFunc<&pg_convert>, true, false, TArgs_pg_convert_Policy>; }
struct TArgs_pg_convert_from_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_convert_from() { return &GenericExec<TPgDirectFunc<&pg_convert_from>, true, false, TArgs_pg_convert_from_Policy>; }
struct TArgs_pg_convert_to_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_convert_to() { return &GenericExec<TPgDirectFunc<&pg_convert_to>, true, false, TArgs_pg_convert_to_Policy>; }
struct TArgs_pg_copy_logical_replication_slot_a_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,false};
};
TExecFunc arrow_pg_copy_logical_replication_slot_a() { return &GenericExec<TPgDirectFunc<&pg_copy_logical_replication_slot_a>, true, false, TArgs_pg_copy_logical_replication_slot_a_Policy>; }
struct TArgs_pg_copy_logical_replication_slot_b_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_pg_copy_logical_replication_slot_b() { return &GenericExec<TPgDirectFunc<&pg_copy_logical_replication_slot_b>, true, false, TArgs_pg_copy_logical_replication_slot_b_Policy>; }
struct TArgs_pg_copy_logical_replication_slot_c_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_copy_logical_replication_slot_c() { return &GenericExec<TPgDirectFunc<&pg_copy_logical_replication_slot_c>, true, false, TArgs_pg_copy_logical_replication_slot_c_Policy>; }
struct TArgs_pg_copy_physical_replication_slot_a_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_pg_copy_physical_replication_slot_a() { return &GenericExec<TPgDirectFunc<&pg_copy_physical_replication_slot_a>, true, false, TArgs_pg_copy_physical_replication_slot_a_Policy>; }
struct TArgs_pg_copy_physical_replication_slot_b_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_copy_physical_replication_slot_b() { return &GenericExec<TPgDirectFunc<&pg_copy_physical_replication_slot_b>, true, false, TArgs_pg_copy_physical_replication_slot_b_Policy>; }
struct TArgs_pg_create_logical_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_pg_create_logical_replication_slot() { return &GenericExec<TPgDirectFunc<&pg_create_logical_replication_slot>, true, false, TArgs_pg_create_logical_replication_slot_Policy>; }
struct TArgs_pg_create_physical_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_create_physical_replication_slot() { return &GenericExec<TPgDirectFunc<&pg_create_physical_replication_slot>, true, false, TArgs_pg_create_physical_replication_slot_Policy>; }
struct TArgs_pg_drop_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_drop_replication_slot() { return &GenericExec<TPgDirectFunc<&pg_drop_replication_slot>, true, true, TArgs_pg_drop_replication_slot_Policy>; }
struct TArgs_pg_encoding_max_length_sql_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_encoding_max_length_sql() { return &GenericExec<TPgDirectFunc<&pg_encoding_max_length_sql>, true, true, TArgs_pg_encoding_max_length_sql_Policy>; }
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
struct TArgs_pg_extension_config_dump_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_extension_config_dump() { return &GenericExec<TPgDirectFunc<&pg_extension_config_dump>, true, true, TArgs_pg_extension_config_dump_Policy>; }
struct TArgs_pg_has_role_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_has_role_id() { return &GenericExec<TPgDirectFunc<&pg_has_role_id>, true, true, TArgs_pg_has_role_id_Policy>; }
struct TArgs_pg_has_role_id_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_pg_has_role_id_id() { return &GenericExec<TPgDirectFunc<&pg_has_role_id_id>, true, true, TArgs_pg_has_role_id_id_Policy>; }
struct TArgs_pg_has_role_id_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_has_role_id_name() { return &GenericExec<TPgDirectFunc<&pg_has_role_id_name>, true, true, TArgs_pg_has_role_id_name_Policy>; }
struct TArgs_pg_has_role_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_has_role_name() { return &GenericExec<TPgDirectFunc<&pg_has_role_name>, true, true, TArgs_pg_has_role_name_Policy>; }
struct TArgs_pg_has_role_name_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_pg_has_role_name_id() { return &GenericExec<TPgDirectFunc<&pg_has_role_name_id>, true, true, TArgs_pg_has_role_name_id_Policy>; }
struct TArgs_pg_has_role_name_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_pg_has_role_name_name() { return &GenericExec<TPgDirectFunc<&pg_has_role_name_name>, true, true, TArgs_pg_has_role_name_name_Policy>; }
struct TArgs_pg_nextoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_pg_nextoid() { return &GenericExec<TPgDirectFunc<&pg_nextoid>, true, true, TArgs_pg_nextoid_Policy>; }
struct TArgs_pg_replication_slot_advance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_replication_slot_advance() { return &GenericExec<TPgDirectFunc<&pg_replication_slot_advance>, true, false, TArgs_pg_replication_slot_advance_Policy>; }
struct TArgs_pg_trigger_depth_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_trigger_depth() { return &GenericExec<TPgDirectFunc<&pg_trigger_depth>, true, true, TArgs_pg_trigger_depth_Policy>; }
struct TArgs_pgsql_version_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pgsql_version() { return &GenericExec<TPgDirectFunc<&pgsql_version>, true, false, TArgs_pgsql_version_Policy>; }
struct TArgs_point_above_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_above() { return &GenericExec<TPgDirectFunc<&point_above>, true, true, TArgs_point_above_Policy>; }
struct TArgs_point_add_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_add() { return &GenericExec<TPgDirectFunc<&point_add>, true, false, TArgs_point_add_Policy>; }
struct TArgs_point_below_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_below() { return &GenericExec<TPgDirectFunc<&point_below>, true, true, TArgs_point_below_Policy>; }
struct TArgs_point_box_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_point_box() { return &GenericExec<TPgDirectFunc<&point_box>, true, false, TArgs_point_box_Policy>; }
struct TArgs_point_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_distance() { return &GenericExec<TPgDirectFunc<&point_distance>, true, true, TArgs_point_distance_Policy>; }
struct TArgs_point_div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_div() { return &GenericExec<TPgDirectFunc<&point_div>, true, false, TArgs_point_div_Policy>; }
struct TArgs_point_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_eq() { return &GenericExec<TPgDirectFunc<&point_eq>, true, true, TArgs_point_eq_Policy>; }
struct TArgs_point_horiz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_horiz() { return &GenericExec<TPgDirectFunc<&point_horiz>, true, true, TArgs_point_horiz_Policy>; }
struct TArgs_point_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_point_in() { return &GenericExec<TPgDirectFunc<&point_in>, true, false, TArgs_point_in_Policy>; }
struct TArgs_point_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_left() { return &GenericExec<TPgDirectFunc<&point_left>, true, true, TArgs_point_left_Policy>; }
struct TArgs_point_mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_mul() { return &GenericExec<TPgDirectFunc<&point_mul>, true, false, TArgs_point_mul_Policy>; }
struct TArgs_point_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_ne() { return &GenericExec<TPgDirectFunc<&point_ne>, true, true, TArgs_point_ne_Policy>; }
struct TArgs_point_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_point_out() { return &GenericExec<TPgDirectFunc<&point_out>, true, false, TArgs_point_out_Policy>; }
struct TArgs_point_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_point_recv() { return &GenericExec<TPgDirectFunc<&point_recv>, true, false, TArgs_point_recv_Policy>; }
struct TArgs_point_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_right() { return &GenericExec<TPgDirectFunc<&point_right>, true, true, TArgs_point_right_Policy>; }
struct TArgs_point_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_point_send() { return &GenericExec<TPgDirectFunc<&point_send>, true, false, TArgs_point_send_Policy>; }
struct TArgs_point_slope_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_slope() { return &GenericExec<TPgDirectFunc<&point_slope>, true, true, TArgs_point_slope_Policy>; }
struct TArgs_point_sub_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_sub() { return &GenericExec<TPgDirectFunc<&point_sub>, true, false, TArgs_point_sub_Policy>; }
struct TArgs_point_vert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_point_vert() { return &GenericExec<TPgDirectFunc<&point_vert>, true, true, TArgs_point_vert_Policy>; }
struct TArgs_points_box_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_points_box() { return &GenericExec<TPgDirectFunc<&points_box>, true, false, TArgs_points_box_Policy>; }
struct TArgs_poly_above_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_above() { return &GenericExec<TPgDirectFunc<&poly_above>, true, true, TArgs_poly_above_Policy>; }
struct TArgs_poly_below_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_below() { return &GenericExec<TPgDirectFunc<&poly_below>, true, true, TArgs_poly_below_Policy>; }
struct TArgs_poly_box_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_box() { return &GenericExec<TPgDirectFunc<&poly_box>, true, false, TArgs_poly_box_Policy>; }
struct TArgs_poly_center_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_center() { return &GenericExec<TPgDirectFunc<&poly_center>, true, false, TArgs_poly_center_Policy>; }
struct TArgs_poly_circle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_circle() { return &GenericExec<TPgDirectFunc<&poly_circle>, true, false, TArgs_poly_circle_Policy>; }
struct TArgs_poly_contain_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_contain() { return &GenericExec<TPgDirectFunc<&poly_contain>, true, true, TArgs_poly_contain_Policy>; }
struct TArgs_poly_contain_pt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_contain_pt() { return &GenericExec<TPgDirectFunc<&poly_contain_pt>, true, true, TArgs_poly_contain_pt_Policy>; }
struct TArgs_poly_contained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_contained() { return &GenericExec<TPgDirectFunc<&poly_contained>, true, true, TArgs_poly_contained_Policy>; }
struct TArgs_poly_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_distance() { return &GenericExec<TPgDirectFunc<&poly_distance>, true, true, TArgs_poly_distance_Policy>; }
struct TArgs_poly_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_in() { return &GenericExec<TPgDirectFunc<&poly_in>, true, false, TArgs_poly_in_Policy>; }
struct TArgs_poly_left_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_left() { return &GenericExec<TPgDirectFunc<&poly_left>, true, true, TArgs_poly_left_Policy>; }
struct TArgs_poly_npoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_npoints() { return &GenericExec<TPgDirectFunc<&poly_npoints>, true, true, TArgs_poly_npoints_Policy>; }
struct TArgs_poly_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_out() { return &GenericExec<TPgDirectFunc<&poly_out>, true, false, TArgs_poly_out_Policy>; }
struct TArgs_poly_overabove_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_overabove() { return &GenericExec<TPgDirectFunc<&poly_overabove>, true, true, TArgs_poly_overabove_Policy>; }
struct TArgs_poly_overbelow_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_overbelow() { return &GenericExec<TPgDirectFunc<&poly_overbelow>, true, true, TArgs_poly_overbelow_Policy>; }
struct TArgs_poly_overlap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_overlap() { return &GenericExec<TPgDirectFunc<&poly_overlap>, true, true, TArgs_poly_overlap_Policy>; }
struct TArgs_poly_overleft_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_overleft() { return &GenericExec<TPgDirectFunc<&poly_overleft>, true, true, TArgs_poly_overleft_Policy>; }
struct TArgs_poly_overright_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_overright() { return &GenericExec<TPgDirectFunc<&poly_overright>, true, true, TArgs_poly_overright_Policy>; }
struct TArgs_poly_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_path() { return &GenericExec<TPgDirectFunc<&poly_path>, true, false, TArgs_poly_path_Policy>; }
struct TArgs_poly_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_poly_recv() { return &GenericExec<TPgDirectFunc<&poly_recv>, true, false, TArgs_poly_recv_Policy>; }
struct TArgs_poly_right_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_right() { return &GenericExec<TPgDirectFunc<&poly_right>, true, true, TArgs_poly_right_Policy>; }
struct TArgs_poly_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_poly_same() { return &GenericExec<TPgDirectFunc<&poly_same>, true, true, TArgs_poly_same_Policy>; }
struct TArgs_poly_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_poly_send() { return &GenericExec<TPgDirectFunc<&poly_send>, true, false, TArgs_poly_send_Policy>; }
struct TArgs_pt_contained_circle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pt_contained_circle() { return &GenericExec<TPgDirectFunc<&pt_contained_circle>, true, true, TArgs_pt_contained_circle_Policy>; }
struct TArgs_pt_contained_poly_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pt_contained_poly() { return &GenericExec<TPgDirectFunc<&pt_contained_poly>, true, true, TArgs_pt_contained_poly_Policy>; }
struct TArgs_quote_ident_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_quote_ident() { return &GenericExec<TPgDirectFunc<&quote_ident>, true, false, TArgs_quote_ident_Policy>; }
struct TArgs_quote_literal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_quote_literal() { return &GenericExec<TPgDirectFunc<&quote_literal>, true, false, TArgs_quote_literal_Policy>; }
struct TArgs_quote_nullable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_quote_nullable() { return &GenericExec<TPgDirectFunc<&quote_nullable>, false, false, TArgs_quote_nullable_Policy>; }
struct TArgs_range_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_range_typanalyze() { return &GenericExec<TPgDirectFunc<&range_typanalyze>, true, true, TArgs_range_typanalyze_Policy>; }
struct TArgs_repeat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_repeat() { return &GenericExec<TPgDirectFunc<&repeat>, true, false, TArgs_repeat_Policy>; }
struct TArgs_row_to_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_row_to_json() { return &GenericExec<TPgDirectFunc<&row_to_json>, true, false, TArgs_row_to_json_Policy>; }
struct TArgs_row_to_json_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_row_to_json_pretty() { return &GenericExec<TPgDirectFunc<&row_to_json_pretty>, true, false, TArgs_row_to_json_pretty_Policy>; }
struct TArgs_rpad_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_rpad() { return &GenericExec<TPgDirectFunc<&rpad>, true, false, TArgs_rpad_Policy>; }
struct TArgs_rtrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_rtrim() { return &GenericExec<TPgDirectFunc<&rtrim>, true, false, TArgs_rtrim_Policy>; }
struct TArgs_rtrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_rtrim1() { return &GenericExec<TPgDirectFunc<&rtrim1>, true, false, TArgs_rtrim1_Policy>; }
struct TArgs_scalargejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_scalargejoinsel() { return &GenericExec<TPgDirectFunc<&scalargejoinsel>, true, true, TArgs_scalargejoinsel_Policy>; }
struct TArgs_scalargesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_scalargesel() { return &GenericExec<TPgDirectFunc<&scalargesel>, true, true, TArgs_scalargesel_Policy>; }
struct TArgs_scalargtjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_scalargtjoinsel() { return &GenericExec<TPgDirectFunc<&scalargtjoinsel>, true, true, TArgs_scalargtjoinsel_Policy>; }
struct TArgs_scalargtsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_scalargtsel() { return &GenericExec<TPgDirectFunc<&scalargtsel>, true, true, TArgs_scalargtsel_Policy>; }
struct TArgs_scalarlejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_scalarlejoinsel() { return &GenericExec<TPgDirectFunc<&scalarlejoinsel>, true, true, TArgs_scalarlejoinsel_Policy>; }
struct TArgs_scalarlesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_scalarlesel() { return &GenericExec<TPgDirectFunc<&scalarlesel>, true, true, TArgs_scalarlesel_Policy>; }
struct TArgs_scalarltjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_scalarltjoinsel() { return &GenericExec<TPgDirectFunc<&scalarltjoinsel>, true, true, TArgs_scalarltjoinsel_Policy>; }
struct TArgs_scalarltsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_scalarltsel() { return &GenericExec<TPgDirectFunc<&scalarltsel>, true, true, TArgs_scalarltsel_Policy>; }
struct TArgs_set_config_by_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_set_config_by_name() { return &GenericExec<TPgDirectFunc<&set_config_by_name>, false, false, TArgs_set_config_by_name_Policy>; }
struct TArgs_sha224_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_sha224_bytea() { return &GenericExec<TPgDirectFunc<&sha224_bytea>, true, false, TArgs_sha224_bytea_Policy>; }
struct TArgs_sha256_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_sha256_bytea() { return &GenericExec<TPgDirectFunc<&sha256_bytea>, true, false, TArgs_sha256_bytea_Policy>; }
struct TArgs_sha384_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_sha384_bytea() { return &GenericExec<TPgDirectFunc<&sha384_bytea>, true, false, TArgs_sha384_bytea_Policy>; }
struct TArgs_sha512_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_sha512_bytea() { return &GenericExec<TPgDirectFunc<&sha512_bytea>, true, false, TArgs_sha512_bytea_Policy>; }
struct TArgs_show_config_by_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_show_config_by_name() { return &GenericExec<TPgDirectFunc<&show_config_by_name>, true, false, TArgs_show_config_by_name_Policy>; }
struct TArgs_show_config_by_name_missing_ok_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_show_config_by_name_missing_ok() { return &GenericExec<TPgDirectFunc<&show_config_by_name_missing_ok>, true, false, TArgs_show_config_by_name_missing_ok_Policy>; }
struct TArgs_spg_kd_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_choose() { return &GenericExec<TPgDirectFunc<&spg_kd_choose>, true, true, TArgs_spg_kd_choose_Policy>; }
struct TArgs_spg_kd_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_config() { return &GenericExec<TPgDirectFunc<&spg_kd_config>, true, true, TArgs_spg_kd_config_Policy>; }
struct TArgs_spg_kd_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_inner_consistent() { return &GenericExec<TPgDirectFunc<&spg_kd_inner_consistent>, true, true, TArgs_spg_kd_inner_consistent_Policy>; }
struct TArgs_spg_kd_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_picksplit() { return &GenericExec<TPgDirectFunc<&spg_kd_picksplit>, true, true, TArgs_spg_kd_picksplit_Policy>; }
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
struct TArgs_texticlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticlike() { return &GenericExec<TPgDirectFunc<&texticlike>, true, true, TArgs_texticlike_Policy>; }
struct TArgs_texticnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticnlike() { return &GenericExec<TPgDirectFunc<&texticnlike>, true, true, TArgs_texticnlike_Policy>; }
struct TArgs_textlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textlike() { return &GenericExec<TPgDirectFunc<&textlike>, true, true, TArgs_textlike_Policy>; }
struct TArgs_textnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textnlike() { return &GenericExec<TPgDirectFunc<&textnlike>, true, true, TArgs_textnlike_Policy>; }
struct TArgs_timestamp_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamp_to_char() { return &GenericExec<TPgDirectFunc<&timestamp_to_char>, true, false, TArgs_timestamp_to_char_Policy>; }
struct TArgs_timestamptz_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamptz_to_char() { return &GenericExec<TPgDirectFunc<&timestamptz_to_char>, true, false, TArgs_timestamptz_to_char_Policy>; }
struct TArgs_to_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_to_date() { return &GenericExec<TPgDirectFunc<&to_date>, true, true, TArgs_to_date_Policy>; }
struct TArgs_to_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_json() { return &GenericExec<TPgDirectFunc<&to_json>, true, false, TArgs_to_json_Policy>; }
struct TArgs_to_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_jsonb() { return &GenericExec<TPgDirectFunc<&to_jsonb>, true, false, TArgs_to_jsonb_Policy>; }
struct TArgs_to_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_to_timestamp() { return &GenericExec<TPgDirectFunc<&to_timestamp>, true, true, TArgs_to_timestamp_Policy>; }
struct TArgs_translate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_translate() { return &GenericExec<TPgDirectFunc<&translate>, true, false, TArgs_translate_Policy>; }
struct TArgs_trim_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_trim_array() { return &GenericExec<TPgDirectFunc<&trim_array>, true, false, TArgs_trim_array_Policy>; }
struct TArgs_ts_match_qv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_qv() { return &GenericExec<TPgDirectFunc<&ts_match_qv>, true, true, TArgs_ts_match_qv_Policy>; }
struct TArgs_ts_match_tq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_tq() { return &GenericExec<TPgDirectFunc<&ts_match_tq>, true, true, TArgs_ts_match_tq_Policy>; }
struct TArgs_ts_match_tt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_tt() { return &GenericExec<TPgDirectFunc<&ts_match_tt>, true, true, TArgs_ts_match_tt_Policy>; }
struct TArgs_ts_match_vq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_ts_match_vq() { return &GenericExec<TPgDirectFunc<&ts_match_vq>, true, true, TArgs_ts_match_vq_Policy>; }
struct TArgs_tsmatchjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_tsmatchjoinsel() { return &GenericExec<TPgDirectFunc<&tsmatchjoinsel>, true, true, TArgs_tsmatchjoinsel_Policy>; }
struct TArgs_tsmatchsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_tsmatchsel() { return &GenericExec<TPgDirectFunc<&tsmatchsel>, true, true, TArgs_tsmatchsel_Policy>; }
struct TArgs_tsq_mcontained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsq_mcontained() { return &GenericExec<TPgDirectFunc<&tsq_mcontained>, true, true, TArgs_tsq_mcontained_Policy>; }
struct TArgs_tsq_mcontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsq_mcontains() { return &GenericExec<TPgDirectFunc<&tsq_mcontains>, true, true, TArgs_tsq_mcontains_Policy>; }
struct TArgs_tsquery_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_and() { return &GenericExec<TPgDirectFunc<&tsquery_and>, true, false, TArgs_tsquery_and_Policy>; }
struct TArgs_tsquery_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_cmp() { return &GenericExec<TPgDirectFunc<&tsquery_cmp>, true, true, TArgs_tsquery_cmp_Policy>; }
struct TArgs_tsquery_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_eq() { return &GenericExec<TPgDirectFunc<&tsquery_eq>, true, true, TArgs_tsquery_eq_Policy>; }
struct TArgs_tsquery_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_ge() { return &GenericExec<TPgDirectFunc<&tsquery_ge>, true, true, TArgs_tsquery_ge_Policy>; }
struct TArgs_tsquery_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_gt() { return &GenericExec<TPgDirectFunc<&tsquery_gt>, true, true, TArgs_tsquery_gt_Policy>; }
struct TArgs_tsquery_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_le() { return &GenericExec<TPgDirectFunc<&tsquery_le>, true, true, TArgs_tsquery_le_Policy>; }
struct TArgs_tsquery_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_lt() { return &GenericExec<TPgDirectFunc<&tsquery_lt>, true, true, TArgs_tsquery_lt_Policy>; }
struct TArgs_tsquery_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_ne() { return &GenericExec<TPgDirectFunc<&tsquery_ne>, true, true, TArgs_tsquery_ne_Policy>; }
struct TArgs_tsquery_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquery_not() { return &GenericExec<TPgDirectFunc<&tsquery_not>, true, false, TArgs_tsquery_not_Policy>; }
struct TArgs_tsquery_numnode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquery_numnode() { return &GenericExec<TPgDirectFunc<&tsquery_numnode>, true, true, TArgs_tsquery_numnode_Policy>; }
struct TArgs_tsquery_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_or() { return &GenericExec<TPgDirectFunc<&tsquery_or>, true, false, TArgs_tsquery_or_Policy>; }
struct TArgs_tsquery_phrase_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_phrase() { return &GenericExec<TPgDirectFunc<&tsquery_phrase>, true, false, TArgs_tsquery_phrase_Policy>; }
struct TArgs_tsquery_phrase_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_tsquery_phrase_distance() { return &GenericExec<TPgDirectFunc<&tsquery_phrase_distance>, true, false, TArgs_tsquery_phrase_distance_Policy>; }
struct TArgs_tsvector_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_cmp() { return &GenericExec<TPgDirectFunc<&tsvector_cmp>, true, true, TArgs_tsvector_cmp_Policy>; }
struct TArgs_tsvector_concat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_concat() { return &GenericExec<TPgDirectFunc<&tsvector_concat>, true, false, TArgs_tsvector_concat_Policy>; }
struct TArgs_tsvector_delete_arr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_delete_arr() { return &GenericExec<TPgDirectFunc<&tsvector_delete_arr>, true, false, TArgs_tsvector_delete_arr_Policy>; }
struct TArgs_tsvector_delete_str_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_delete_str() { return &GenericExec<TPgDirectFunc<&tsvector_delete_str>, true, false, TArgs_tsvector_delete_str_Policy>; }
struct TArgs_tsvector_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_eq() { return &GenericExec<TPgDirectFunc<&tsvector_eq>, true, true, TArgs_tsvector_eq_Policy>; }
struct TArgs_tsvector_filter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_filter() { return &GenericExec<TPgDirectFunc<&tsvector_filter>, true, false, TArgs_tsvector_filter_Policy>; }
struct TArgs_tsvector_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_ge() { return &GenericExec<TPgDirectFunc<&tsvector_ge>, true, true, TArgs_tsvector_ge_Policy>; }
struct TArgs_tsvector_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_gt() { return &GenericExec<TPgDirectFunc<&tsvector_gt>, true, true, TArgs_tsvector_gt_Policy>; }
struct TArgs_tsvector_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_le() { return &GenericExec<TPgDirectFunc<&tsvector_le>, true, true, TArgs_tsvector_le_Policy>; }
struct TArgs_tsvector_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_length() { return &GenericExec<TPgDirectFunc<&tsvector_length>, true, true, TArgs_tsvector_length_Policy>; }
struct TArgs_tsvector_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_lt() { return &GenericExec<TPgDirectFunc<&tsvector_lt>, true, true, TArgs_tsvector_lt_Policy>; }
struct TArgs_tsvector_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsvector_ne() { return &GenericExec<TPgDirectFunc<&tsvector_ne>, true, true, TArgs_tsvector_ne_Policy>; }
struct TArgs_tsvector_setweight_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_tsvector_setweight() { return &GenericExec<TPgDirectFunc<&tsvector_setweight>, true, false, TArgs_tsvector_setweight_Policy>; }
struct TArgs_tsvector_setweight_by_filter_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_tsvector_setweight_by_filter() { return &GenericExec<TPgDirectFunc<&tsvector_setweight_by_filter>, true, false, TArgs_tsvector_setweight_by_filter_Policy>; }
struct TArgs_tsvector_strip_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_strip() { return &GenericExec<TPgDirectFunc<&tsvector_strip>, true, false, TArgs_tsvector_strip_Policy>; }
struct TArgs_tsvector_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvector_to_array() { return &GenericExec<TPgDirectFunc<&tsvector_to_array>, true, false, TArgs_tsvector_to_array_Policy>; }
struct TArgs_tsvector_update_trigger_bycolumn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_tsvector_update_trigger_bycolumn() { return &GenericExec<TPgDirectFunc<&tsvector_update_trigger_bycolumn>, false, true, TArgs_tsvector_update_trigger_bycolumn_Policy>; }
struct TArgs_tsvector_update_trigger_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_tsvector_update_trigger_byid() { return &GenericExec<TPgDirectFunc<&tsvector_update_trigger_byid>, false, true, TArgs_tsvector_update_trigger_byid_Policy>; }
struct TArgs_tsvectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorin() { return &GenericExec<TPgDirectFunc<&tsvectorin>, true, false, TArgs_tsvectorin_Policy>; }
struct TArgs_tsvectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorout() { return &GenericExec<TPgDirectFunc<&tsvectorout>, true, false, TArgs_tsvectorout_Policy>; }
struct TArgs_tsvectorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsvectorrecv() { return &GenericExec<TPgDirectFunc<&tsvectorrecv>, true, false, TArgs_tsvectorrecv_Policy>; }
struct TArgs_tsvectorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorsend() { return &GenericExec<TPgDirectFunc<&tsvectorsend>, true, false, TArgs_tsvectorsend_Policy>; }
struct TArgs_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_upper() { return &GenericExec<TPgDirectFunc<&upper>, true, false, TArgs_upper_Policy>; }
struct TArgs_uuid_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_cmp() { return &GenericExec<TPgDirectFunc<&uuid_cmp>, true, true, TArgs_uuid_cmp_Policy>; }
struct TArgs_uuid_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_eq() { return &GenericExec<TPgDirectFunc<&uuid_eq>, true, true, TArgs_uuid_eq_Policy>; }
struct TArgs_uuid_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_ge() { return &GenericExec<TPgDirectFunc<&uuid_ge>, true, true, TArgs_uuid_ge_Policy>; }
struct TArgs_uuid_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_gt() { return &GenericExec<TPgDirectFunc<&uuid_gt>, true, true, TArgs_uuid_gt_Policy>; }
struct TArgs_uuid_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_uuid_hash() { return &GenericExec<TPgDirectFunc<&uuid_hash>, true, true, TArgs_uuid_hash_Policy>; }
struct TArgs_uuid_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_uuid_hash_extended() { return &GenericExec<TPgDirectFunc<&uuid_hash_extended>, true, true, TArgs_uuid_hash_extended_Policy>; }
struct TArgs_uuid_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_uuid_in() { return &GenericExec<TPgDirectFunc<&uuid_in>, true, false, TArgs_uuid_in_Policy>; }
struct TArgs_uuid_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_le() { return &GenericExec<TPgDirectFunc<&uuid_le>, true, true, TArgs_uuid_le_Policy>; }
struct TArgs_uuid_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_lt() { return &GenericExec<TPgDirectFunc<&uuid_lt>, true, true, TArgs_uuid_lt_Policy>; }
struct TArgs_uuid_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_uuid_ne() { return &GenericExec<TPgDirectFunc<&uuid_ne>, true, true, TArgs_uuid_ne_Policy>; }
struct TArgs_uuid_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_uuid_out() { return &GenericExec<TPgDirectFunc<&uuid_out>, true, false, TArgs_uuid_out_Policy>; }
struct TArgs_uuid_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_uuid_recv() { return &GenericExec<TPgDirectFunc<&uuid_recv>, true, false, TArgs_uuid_recv_Policy>; }
struct TArgs_uuid_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_uuid_send() { return &GenericExec<TPgDirectFunc<&uuid_send>, true, false, TArgs_uuid_send_Policy>; }
struct TArgs_uuid_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_uuid_sortsupport() { return &GenericExec<TPgDirectFunc<&uuid_sortsupport>, true, true, TArgs_uuid_sortsupport_Policy>; }
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
struct TArgs_width_bucket_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_width_bucket_array() { return &GenericExec<TPgDirectFunc<&width_bucket_array>, true, true, TArgs_width_bucket_array_Policy>; }
