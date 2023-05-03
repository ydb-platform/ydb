struct TArgs_RI_FKey_cascade_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_cascade_del() { return TGenericExec<TPgDirectFunc<&RI_FKey_cascade_del>, true, true, TArgs_RI_FKey_cascade_del_Policy>({}); }
struct TArgs_RI_FKey_cascade_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_cascade_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_cascade_upd>, true, true, TArgs_RI_FKey_cascade_upd_Policy>({}); }
struct TArgs_RI_FKey_check_ins_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_check_ins() { return TGenericExec<TPgDirectFunc<&RI_FKey_check_ins>, true, true, TArgs_RI_FKey_check_ins_Policy>({}); }
struct TArgs_RI_FKey_check_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_check_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_check_upd>, true, true, TArgs_RI_FKey_check_upd_Policy>({}); }
struct TArgs_RI_FKey_noaction_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_noaction_del() { return TGenericExec<TPgDirectFunc<&RI_FKey_noaction_del>, true, true, TArgs_RI_FKey_noaction_del_Policy>({}); }
struct TArgs_RI_FKey_noaction_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_noaction_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_noaction_upd>, true, true, TArgs_RI_FKey_noaction_upd_Policy>({}); }
struct TArgs_RI_FKey_restrict_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_restrict_del() { return TGenericExec<TPgDirectFunc<&RI_FKey_restrict_del>, true, true, TArgs_RI_FKey_restrict_del_Policy>({}); }
struct TArgs_RI_FKey_restrict_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_restrict_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_restrict_upd>, true, true, TArgs_RI_FKey_restrict_upd_Policy>({}); }
struct TArgs_RI_FKey_setdefault_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setdefault_del() { return TGenericExec<TPgDirectFunc<&RI_FKey_setdefault_del>, true, true, TArgs_RI_FKey_setdefault_del_Policy>({}); }
struct TArgs_RI_FKey_setdefault_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setdefault_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_setdefault_upd>, true, true, TArgs_RI_FKey_setdefault_upd_Policy>({}); }
struct TArgs_RI_FKey_setnull_del_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setnull_del() { return TGenericExec<TPgDirectFunc<&RI_FKey_setnull_del>, true, true, TArgs_RI_FKey_setnull_del_Policy>({}); }
struct TArgs_RI_FKey_setnull_upd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_RI_FKey_setnull_upd() { return TGenericExec<TPgDirectFunc<&RI_FKey_setnull_upd>, true, true, TArgs_RI_FKey_setnull_upd_Policy>({}); }
struct TArgs_any_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_any_in() { return TGenericExec<TPgDirectFunc<&any_in>, true, true, TArgs_any_in_Policy>({}); }
struct TArgs_any_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_any_out() { return TGenericExec<TPgDirectFunc<&any_out>, true, false, TArgs_any_out_Policy>({}); }
struct TArgs_anyarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_in() { return TGenericExec<TPgDirectFunc<&anyarray_in>, true, false, TArgs_anyarray_in_Policy>({}); }
struct TArgs_anyarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_out() { return TGenericExec<TPgDirectFunc<&anyarray_out>, true, false, TArgs_anyarray_out_Policy>({}); }
struct TArgs_anyarray_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyarray_recv() { return TGenericExec<TPgDirectFunc<&anyarray_recv>, true, false, TArgs_anyarray_recv_Policy>({}); }
struct TArgs_anyarray_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_send() { return TGenericExec<TPgDirectFunc<&anyarray_send>, true, false, TArgs_anyarray_send_Policy>({}); }
struct TArgs_anycompatible_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatible_in() { return TGenericExec<TPgDirectFunc<&anycompatible_in>, true, true, TArgs_anycompatible_in_Policy>({}); }
struct TArgs_anycompatible_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatible_out() { return TGenericExec<TPgDirectFunc<&anycompatible_out>, true, false, TArgs_anycompatible_out_Policy>({}); }
struct TArgs_anycompatiblearray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_in() { return TGenericExec<TPgDirectFunc<&anycompatiblearray_in>, true, false, TArgs_anycompatiblearray_in_Policy>({}); }
struct TArgs_anycompatiblearray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_out() { return TGenericExec<TPgDirectFunc<&anycompatiblearray_out>, true, false, TArgs_anycompatiblearray_out_Policy>({}); }
struct TArgs_anycompatiblearray_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatiblearray_recv() { return TGenericExec<TPgDirectFunc<&anycompatiblearray_recv>, true, false, TArgs_anycompatiblearray_recv_Policy>({}); }
struct TArgs_anycompatiblearray_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_send() { return TGenericExec<TPgDirectFunc<&anycompatiblearray_send>, true, false, TArgs_anycompatiblearray_send_Policy>({}); }
struct TArgs_anycompatiblemultirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anycompatiblemultirange_in() { return TGenericExec<TPgDirectFunc<&anycompatiblemultirange_in>, true, false, TArgs_anycompatiblemultirange_in_Policy>({}); }
struct TArgs_anycompatiblemultirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblemultirange_out() { return TGenericExec<TPgDirectFunc<&anycompatiblemultirange_out>, true, false, TArgs_anycompatiblemultirange_out_Policy>({}); }
struct TArgs_anycompatiblenonarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblenonarray_in() { return TGenericExec<TPgDirectFunc<&anycompatiblenonarray_in>, true, true, TArgs_anycompatiblenonarray_in_Policy>({}); }
struct TArgs_anycompatiblenonarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatiblenonarray_out() { return TGenericExec<TPgDirectFunc<&anycompatiblenonarray_out>, true, false, TArgs_anycompatiblenonarray_out_Policy>({}); }
struct TArgs_anycompatiblerange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anycompatiblerange_in() { return TGenericExec<TPgDirectFunc<&anycompatiblerange_in>, true, false, TArgs_anycompatiblerange_in_Policy>({}); }
struct TArgs_anycompatiblerange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblerange_out() { return TGenericExec<TPgDirectFunc<&anycompatiblerange_out>, true, false, TArgs_anycompatiblerange_out_Policy>({}); }
struct TArgs_anyelement_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyelement_in() { return TGenericExec<TPgDirectFunc<&anyelement_in>, true, true, TArgs_anyelement_in_Policy>({}); }
struct TArgs_anyelement_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyelement_out() { return TGenericExec<TPgDirectFunc<&anyelement_out>, true, false, TArgs_anyelement_out_Policy>({}); }
struct TArgs_anyenum_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyenum_in() { return TGenericExec<TPgDirectFunc<&anyenum_in>, true, true, TArgs_anyenum_in_Policy>({}); }
struct TArgs_anyenum_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyenum_out() { return TGenericExec<TPgDirectFunc<&anyenum_out>, true, false, TArgs_anyenum_out_Policy>({}); }
struct TArgs_anymultirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anymultirange_in() { return TGenericExec<TPgDirectFunc<&anymultirange_in>, true, false, TArgs_anymultirange_in_Policy>({}); }
struct TArgs_anymultirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anymultirange_out() { return TGenericExec<TPgDirectFunc<&anymultirange_out>, true, false, TArgs_anymultirange_out_Policy>({}); }
struct TArgs_anynonarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anynonarray_in() { return TGenericExec<TPgDirectFunc<&anynonarray_in>, true, true, TArgs_anynonarray_in_Policy>({}); }
struct TArgs_anynonarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anynonarray_out() { return TGenericExec<TPgDirectFunc<&anynonarray_out>, true, false, TArgs_anynonarray_out_Policy>({}); }
struct TArgs_anyrange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anyrange_in() { return TGenericExec<TPgDirectFunc<&anyrange_in>, true, false, TArgs_anyrange_in_Policy>({}); }
struct TArgs_anyrange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyrange_out() { return TGenericExec<TPgDirectFunc<&anyrange_out>, true, false, TArgs_anyrange_out_Policy>({}); }
struct TArgs_arraycontjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_arraycontjoinsel() { return TGenericExec<TPgDirectFunc<&arraycontjoinsel>, true, true, TArgs_arraycontjoinsel_Policy>({}); }
struct TArgs_arraycontsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_arraycontsel() { return TGenericExec<TPgDirectFunc<&arraycontsel>, true, true, TArgs_arraycontsel_Policy>({}); }
struct TArgs_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bpchar() { return TGenericExec<TPgDirectFunc<&bpchar>, true, false, TArgs_bpchar_Policy>({}); }
struct TArgs_bpchar_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_larger() { return TGenericExec<TPgDirectFunc<&bpchar_larger>, true, false, TArgs_bpchar_larger_Policy>({}); }
struct TArgs_bpchar_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpchar_name() { return TGenericExec<TPgDirectFunc<&bpchar_name>, true, false, TArgs_bpchar_name_Policy>({}); }
struct TArgs_bpchar_pattern_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_ge() { return TGenericExec<TPgDirectFunc<&bpchar_pattern_ge>, true, true, TArgs_bpchar_pattern_ge_Policy>({}); }
struct TArgs_bpchar_pattern_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_gt() { return TGenericExec<TPgDirectFunc<&bpchar_pattern_gt>, true, true, TArgs_bpchar_pattern_gt_Policy>({}); }
struct TArgs_bpchar_pattern_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_le() { return TGenericExec<TPgDirectFunc<&bpchar_pattern_le>, true, true, TArgs_bpchar_pattern_le_Policy>({}); }
struct TArgs_bpchar_pattern_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_pattern_lt() { return TGenericExec<TPgDirectFunc<&bpchar_pattern_lt>, true, true, TArgs_bpchar_pattern_lt_Policy>({}); }
struct TArgs_bpchar_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchar_smaller() { return TGenericExec<TPgDirectFunc<&bpchar_smaller>, true, false, TArgs_bpchar_smaller_Policy>({}); }
struct TArgs_bpchar_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bpchar_sortsupport() { return TGenericExec<TPgDirectFunc<&bpchar_sortsupport>, true, true, TArgs_bpchar_sortsupport_Policy>({}); }
struct TArgs_bpcharcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharcmp() { return TGenericExec<TPgDirectFunc<&bpcharcmp>, true, true, TArgs_bpcharcmp_Policy>({}); }
struct TArgs_bpchareq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchareq() { return TGenericExec<TPgDirectFunc<&bpchareq>, true, true, TArgs_bpchareq_Policy>({}); }
struct TArgs_bpcharge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharge() { return TGenericExec<TPgDirectFunc<&bpcharge>, true, true, TArgs_bpcharge_Policy>({}); }
struct TArgs_bpchargt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpchargt() { return TGenericExec<TPgDirectFunc<&bpchargt>, true, true, TArgs_bpchargt_Policy>({}); }
struct TArgs_bpcharin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bpcharin() { return TGenericExec<TPgDirectFunc<&bpcharin>, true, false, TArgs_bpcharin_Policy>({}); }
struct TArgs_bpcharle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharle() { return TGenericExec<TPgDirectFunc<&bpcharle>, true, true, TArgs_bpcharle_Policy>({}); }
struct TArgs_bpcharlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharlen() { return TGenericExec<TPgDirectFunc<&bpcharlen>, true, true, TArgs_bpcharlen_Policy>({}); }
struct TArgs_bpcharlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharlt() { return TGenericExec<TPgDirectFunc<&bpcharlt>, true, true, TArgs_bpcharlt_Policy>({}); }
struct TArgs_bpcharne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bpcharne() { return TGenericExec<TPgDirectFunc<&bpcharne>, true, true, TArgs_bpcharne_Policy>({}); }
struct TArgs_bpcharoctetlen_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharoctetlen() { return TGenericExec<TPgDirectFunc<&bpcharoctetlen>, true, true, TArgs_bpcharoctetlen_Policy>({}); }
struct TArgs_bpcharout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharout() { return TGenericExec<TPgDirectFunc<&bpcharout>, true, false, TArgs_bpcharout_Policy>({}); }
struct TArgs_bpcharrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_bpcharrecv() { return TGenericExec<TPgDirectFunc<&bpcharrecv>, true, false, TArgs_bpcharrecv_Policy>({}); }
struct TArgs_bpcharsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpcharsend() { return TGenericExec<TPgDirectFunc<&bpcharsend>, true, false, TArgs_bpcharsend_Policy>({}); }
struct TArgs_bpchartypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bpchartypmodin() { return TGenericExec<TPgDirectFunc<&bpchartypmodin>, true, true, TArgs_bpchartypmodin_Policy>({}); }
struct TArgs_bpchartypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bpchartypmodout() { return TGenericExec<TPgDirectFunc<&bpchartypmodout>, true, false, TArgs_bpchartypmodout_Policy>({}); }
struct TArgs_brin_minmax_add_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_brin_minmax_add_value() { return TGenericExec<TPgDirectFunc<&brin_minmax_add_value>, true, true, TArgs_brin_minmax_add_value_Policy>({}); }
struct TArgs_brin_minmax_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_minmax_consistent() { return TGenericExec<TPgDirectFunc<&brin_minmax_consistent>, true, true, TArgs_brin_minmax_consistent_Policy>({}); }
struct TArgs_brin_minmax_opcinfo_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_brin_minmax_opcinfo() { return TGenericExec<TPgDirectFunc<&brin_minmax_opcinfo>, true, true, TArgs_brin_minmax_opcinfo_Policy>({}); }
struct TArgs_brin_minmax_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_brin_minmax_union() { return TGenericExec<TPgDirectFunc<&brin_minmax_union>, true, true, TArgs_brin_minmax_union_Policy>({}); }
struct TArgs_btbpchar_pattern_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btbpchar_pattern_cmp() { return TGenericExec<TPgDirectFunc<&btbpchar_pattern_cmp>, true, true, TArgs_btbpchar_pattern_cmp_Policy>({}); }
struct TArgs_btbpchar_pattern_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btbpchar_pattern_sortsupport() { return TGenericExec<TPgDirectFunc<&btbpchar_pattern_sortsupport>, true, true, TArgs_btbpchar_pattern_sortsupport_Policy>({}); }
struct TArgs_btfloat48cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat48cmp() { return TGenericExec<TPgDirectFunc<&btfloat48cmp>, true, true, TArgs_btfloat48cmp_Policy>({}); }
struct TArgs_btfloat4cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat4cmp() { return TGenericExec<TPgDirectFunc<&btfloat4cmp>, true, true, TArgs_btfloat4cmp_Policy>({}); }
struct TArgs_btfloat4sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btfloat4sortsupport() { return TGenericExec<TPgDirectFunc<&btfloat4sortsupport>, true, true, TArgs_btfloat4sortsupport_Policy>({}); }
struct TArgs_btfloat84cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat84cmp() { return TGenericExec<TPgDirectFunc<&btfloat84cmp>, true, true, TArgs_btfloat84cmp_Policy>({}); }
struct TArgs_btfloat8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btfloat8cmp() { return TGenericExec<TPgDirectFunc<&btfloat8cmp>, true, true, TArgs_btfloat8cmp_Policy>({}); }
struct TArgs_btfloat8sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btfloat8sortsupport() { return TGenericExec<TPgDirectFunc<&btfloat8sortsupport>, true, true, TArgs_btfloat8sortsupport_Policy>({}); }
struct TArgs_btnamecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btnamecmp() { return TGenericExec<TPgDirectFunc<&btnamecmp>, true, true, TArgs_btnamecmp_Policy>({}); }
struct TArgs_btnamesortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_btnamesortsupport() { return TGenericExec<TPgDirectFunc<&btnamesortsupport>, true, true, TArgs_btnamesortsupport_Policy>({}); }
struct TArgs_btrecordcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btrecordcmp() { return TGenericExec<TPgDirectFunc<&btrecordcmp>, true, true, TArgs_btrecordcmp_Policy>({}); }
struct TArgs_btrecordimagecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btrecordimagecmp() { return TGenericExec<TPgDirectFunc<&btrecordimagecmp>, true, true, TArgs_btrecordimagecmp_Policy>({}); }
struct TArgs_char_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_char_bpchar() { return TGenericExec<TPgDirectFunc<&char_bpchar>, true, false, TArgs_char_bpchar_Policy>({}); }
struct TArgs_cideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_cideq() { return TGenericExec<TPgDirectFunc<&cideq>, true, true, TArgs_cideq_Policy>({}); }
struct TArgs_cidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cidin() { return TGenericExec<TPgDirectFunc<&cidin>, true, true, TArgs_cidin_Policy>({}); }
struct TArgs_cidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidout() { return TGenericExec<TPgDirectFunc<&cidout>, true, false, TArgs_cidout_Policy>({}); }
struct TArgs_cidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidrecv() { return TGenericExec<TPgDirectFunc<&cidrecv>, true, true, TArgs_cidrecv_Policy>({}); }
struct TArgs_cidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cidsend() { return TGenericExec<TPgDirectFunc<&cidsend>, true, false, TArgs_cidsend_Policy>({}); }
struct TArgs_cstring_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_in() { return TGenericExec<TPgDirectFunc<&cstring_in>, true, false, TArgs_cstring_in_Policy>({}); }
struct TArgs_cstring_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_out() { return TGenericExec<TPgDirectFunc<&cstring_out>, true, false, TArgs_cstring_out_Policy>({}); }
struct TArgs_cstring_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cstring_recv() { return TGenericExec<TPgDirectFunc<&cstring_recv>, true, false, TArgs_cstring_recv_Policy>({}); }
struct TArgs_cstring_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_send() { return TGenericExec<TPgDirectFunc<&cstring_send>, true, false, TArgs_cstring_send_Policy>({}); }
struct TArgs_current_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_schema() { return TGenericExec<TPgDirectFunc<&current_schema>, true, false, TArgs_current_schema_Policy>({}); }
struct TArgs_current_schemas_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_current_schemas() { return TGenericExec<TPgDirectFunc<&current_schemas>, true, false, TArgs_current_schemas_Policy>({}); }
struct TArgs_current_user_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_current_user() { return TGenericExec<TPgDirectFunc<&current_user>, true, false, TArgs_current_user_Policy>({}); }
struct TArgs_currval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_currval_oid() { return TGenericExec<TPgDirectFunc<&currval_oid>, true, true, TArgs_currval_oid_Policy>({}); }
struct TArgs_dacos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacos() { return TGenericExec<TPgDirectFunc<&dacos>, true, true, TArgs_dacos_Policy>({}); }
struct TArgs_dacosd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacosd() { return TGenericExec<TPgDirectFunc<&dacosd>, true, true, TArgs_dacosd_Policy>({}); }
struct TArgs_dacosh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dacosh() { return TGenericExec<TPgDirectFunc<&dacosh>, true, true, TArgs_dacosh_Policy>({}); }
struct TArgs_dasin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasin() { return TGenericExec<TPgDirectFunc<&dasin>, true, true, TArgs_dasin_Policy>({}); }
struct TArgs_dasind_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasind() { return TGenericExec<TPgDirectFunc<&dasind>, true, true, TArgs_dasind_Policy>({}); }
struct TArgs_dasinh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dasinh() { return TGenericExec<TPgDirectFunc<&dasinh>, true, true, TArgs_dasinh_Policy>({}); }
struct TArgs_datan_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datan() { return TGenericExec<TPgDirectFunc<&datan>, true, true, TArgs_datan_Policy>({}); }
struct TArgs_datan2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datan2() { return TGenericExec<TPgDirectFunc<&datan2>, true, true, TArgs_datan2_Policy>({}); }
struct TArgs_datan2d_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_datan2d() { return TGenericExec<TPgDirectFunc<&datan2d>, true, true, TArgs_datan2d_Policy>({}); }
struct TArgs_datand_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datand() { return TGenericExec<TPgDirectFunc<&datand>, true, true, TArgs_datand_Policy>({}); }
struct TArgs_datanh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_datanh() { return TGenericExec<TPgDirectFunc<&datanh>, true, true, TArgs_datanh_Policy>({}); }
struct TArgs_dcbrt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcbrt() { return TGenericExec<TPgDirectFunc<&dcbrt>, true, true, TArgs_dcbrt_Policy>({}); }
struct TArgs_dceil_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dceil() { return TGenericExec<TPgDirectFunc<&dceil>, true, true, TArgs_dceil_Policy>({}); }
struct TArgs_dcos_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcos() { return TGenericExec<TPgDirectFunc<&dcos>, true, true, TArgs_dcos_Policy>({}); }
struct TArgs_dcosd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcosd() { return TGenericExec<TPgDirectFunc<&dcosd>, true, true, TArgs_dcosd_Policy>({}); }
struct TArgs_dcosh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcosh() { return TGenericExec<TPgDirectFunc<&dcosh>, true, true, TArgs_dcosh_Policy>({}); }
struct TArgs_dcot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcot() { return TGenericExec<TPgDirectFunc<&dcot>, true, true, TArgs_dcot_Policy>({}); }
struct TArgs_dcotd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dcotd() { return TGenericExec<TPgDirectFunc<&dcotd>, true, true, TArgs_dcotd_Policy>({}); }
struct TArgs_degrees_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_degrees() { return TGenericExec<TPgDirectFunc<&degrees>, true, true, TArgs_degrees_Policy>({}); }
struct TArgs_dexp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dexp() { return TGenericExec<TPgDirectFunc<&dexp>, true, true, TArgs_dexp_Policy>({}); }
struct TArgs_dfloor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dfloor() { return TGenericExec<TPgDirectFunc<&dfloor>, true, true, TArgs_dfloor_Policy>({}); }
struct TArgs_dispell_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dispell_init() { return TGenericExec<TPgDirectFunc<&dispell_init>, true, true, TArgs_dispell_init_Policy>({}); }
struct TArgs_dispell_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dispell_lexize() { return TGenericExec<TPgDirectFunc<&dispell_lexize>, true, true, TArgs_dispell_lexize_Policy>({}); }
struct TArgs_dlog1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dlog1() { return TGenericExec<TPgDirectFunc<&dlog1>, true, true, TArgs_dlog1_Policy>({}); }
struct TArgs_dlog10_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dlog10() { return TGenericExec<TPgDirectFunc<&dlog10>, true, true, TArgs_dlog10_Policy>({}); }
struct TArgs_domain_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_domain_in() { return TGenericExec<TPgDirectFunc<&domain_in>, false, true, TArgs_domain_in_Policy>({}); }
struct TArgs_domain_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_domain_recv() { return TGenericExec<TPgDirectFunc<&domain_recv>, false, true, TArgs_domain_recv_Policy>({}); }
struct TArgs_dpi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_dpi() { return TGenericExec<TPgDirectFunc<&dpi>, true, true, TArgs_dpi_Policy>({}); }
struct TArgs_dpow_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_dpow() { return TGenericExec<TPgDirectFunc<&dpow>, true, true, TArgs_dpow_Policy>({}); }
struct TArgs_drandom_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_drandom() { return TGenericExec<TPgDirectFunc<&drandom>, true, true, TArgs_drandom_Policy>({}); }
struct TArgs_dround_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dround() { return TGenericExec<TPgDirectFunc<&dround>, true, true, TArgs_dround_Policy>({}); }
struct TArgs_dsign_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsign() { return TGenericExec<TPgDirectFunc<&dsign>, true, true, TArgs_dsign_Policy>({}); }
struct TArgs_dsin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsin() { return TGenericExec<TPgDirectFunc<&dsin>, true, true, TArgs_dsin_Policy>({}); }
struct TArgs_dsind_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsind() { return TGenericExec<TPgDirectFunc<&dsind>, true, true, TArgs_dsind_Policy>({}); }
struct TArgs_dsinh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsinh() { return TGenericExec<TPgDirectFunc<&dsinh>, true, true, TArgs_dsinh_Policy>({}); }
struct TArgs_dsqrt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsqrt() { return TGenericExec<TPgDirectFunc<&dsqrt>, true, true, TArgs_dsqrt_Policy>({}); }
struct TArgs_dsynonym_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsynonym_init() { return TGenericExec<TPgDirectFunc<&dsynonym_init>, true, true, TArgs_dsynonym_init_Policy>({}); }
struct TArgs_dsynonym_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dsynonym_lexize() { return TGenericExec<TPgDirectFunc<&dsynonym_lexize>, true, true, TArgs_dsynonym_lexize_Policy>({}); }
struct TArgs_dtan_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtan() { return TGenericExec<TPgDirectFunc<&dtan>, true, true, TArgs_dtan_Policy>({}); }
struct TArgs_dtand_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtand() { return TGenericExec<TPgDirectFunc<&dtand>, true, true, TArgs_dtand_Policy>({}); }
struct TArgs_dtanh_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtanh() { return TGenericExec<TPgDirectFunc<&dtanh>, true, true, TArgs_dtanh_Policy>({}); }
struct TArgs_dtof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtof() { return TGenericExec<TPgDirectFunc<&dtof>, true, true, TArgs_dtof_Policy>({}); }
struct TArgs_dtoi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi2() { return TGenericExec<TPgDirectFunc<&dtoi2>, true, true, TArgs_dtoi2_Policy>({}); }
struct TArgs_dtoi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi4() { return TGenericExec<TPgDirectFunc<&dtoi4>, true, true, TArgs_dtoi4_Policy>({}); }
struct TArgs_dtrunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtrunc() { return TGenericExec<TPgDirectFunc<&dtrunc>, true, true, TArgs_dtrunc_Policy>({}); }
struct TArgs_elem_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_elem_contained_by_multirange() { return TGenericExec<TPgDirectFunc<&elem_contained_by_multirange>, true, true, TArgs_elem_contained_by_multirange_Policy>({}); }
struct TArgs_event_trigger_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_event_trigger_in() { return TGenericExec<TPgDirectFunc<&event_trigger_in>, false, true, TArgs_event_trigger_in_Policy>({}); }
struct TArgs_event_trigger_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_event_trigger_out() { return TGenericExec<TPgDirectFunc<&event_trigger_out>, true, false, TArgs_event_trigger_out_Policy>({}); }
struct TArgs_fdw_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_fdw_handler_in() { return TGenericExec<TPgDirectFunc<&fdw_handler_in>, false, true, TArgs_fdw_handler_in_Policy>({}); }
struct TArgs_fdw_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_fdw_handler_out() { return TGenericExec<TPgDirectFunc<&fdw_handler_out>, true, false, TArgs_fdw_handler_out_Policy>({}); }
struct TArgs_float48div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48div() { return TGenericExec<TPgDirectFunc<&float48div>, true, true, TArgs_float48div_Policy>({}); }
struct TArgs_float48eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48eq() { return TGenericExec<TPgDirectFunc<&float48eq>, true, true, TArgs_float48eq_Policy>({}); }
struct TArgs_float48ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48ge() { return TGenericExec<TPgDirectFunc<&float48ge>, true, true, TArgs_float48ge_Policy>({}); }
struct TArgs_float48gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48gt() { return TGenericExec<TPgDirectFunc<&float48gt>, true, true, TArgs_float48gt_Policy>({}); }
struct TArgs_float48le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48le() { return TGenericExec<TPgDirectFunc<&float48le>, true, true, TArgs_float48le_Policy>({}); }
struct TArgs_float48lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48lt() { return TGenericExec<TPgDirectFunc<&float48lt>, true, true, TArgs_float48lt_Policy>({}); }
struct TArgs_float48mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48mi() { return TGenericExec<TPgDirectFunc<&float48mi>, true, true, TArgs_float48mi_Policy>({}); }
struct TArgs_float48mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48mul() { return TGenericExec<TPgDirectFunc<&float48mul>, true, true, TArgs_float48mul_Policy>({}); }
struct TArgs_float48ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48ne() { return TGenericExec<TPgDirectFunc<&float48ne>, true, true, TArgs_float48ne_Policy>({}); }
struct TArgs_float48pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float48pl() { return TGenericExec<TPgDirectFunc<&float48pl>, true, true, TArgs_float48pl_Policy>({}); }
struct TArgs_float4_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_float4_accum() { return TGenericExec<TPgDirectFunc<&float4_accum>, true, false, TArgs_float4_accum_Policy>({}); }
struct TArgs_float4abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4abs() { return TGenericExec<TPgDirectFunc<&float4abs>, true, true, TArgs_float4abs_Policy>({}); }
struct TArgs_float4div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4div() { return TGenericExec<TPgDirectFunc<&float4div>, true, true, TArgs_float4div_Policy>({}); }
struct TArgs_float4eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4eq() { return TGenericExec<TPgDirectFunc<&float4eq>, true, true, TArgs_float4eq_Policy>({}); }
struct TArgs_float4ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4ge() { return TGenericExec<TPgDirectFunc<&float4ge>, true, true, TArgs_float4ge_Policy>({}); }
struct TArgs_float4gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4gt() { return TGenericExec<TPgDirectFunc<&float4gt>, true, true, TArgs_float4gt_Policy>({}); }
struct TArgs_float4in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float4in() { return TGenericExec<TPgDirectFunc<&float4in>, true, true, TArgs_float4in_Policy>({}); }
struct TArgs_float4larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4larger() { return TGenericExec<TPgDirectFunc<&float4larger>, true, true, TArgs_float4larger_Policy>({}); }
struct TArgs_float4le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4le() { return TGenericExec<TPgDirectFunc<&float4le>, true, true, TArgs_float4le_Policy>({}); }
struct TArgs_float4lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4lt() { return TGenericExec<TPgDirectFunc<&float4lt>, true, true, TArgs_float4lt_Policy>({}); }
struct TArgs_float4mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4mi() { return TGenericExec<TPgDirectFunc<&float4mi>, true, true, TArgs_float4mi_Policy>({}); }
struct TArgs_float4mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4mul() { return TGenericExec<TPgDirectFunc<&float4mul>, true, true, TArgs_float4mul_Policy>({}); }
struct TArgs_float4ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4ne() { return TGenericExec<TPgDirectFunc<&float4ne>, true, true, TArgs_float4ne_Policy>({}); }
struct TArgs_float4out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4out() { return TGenericExec<TPgDirectFunc<&float4out>, true, false, TArgs_float4out_Policy>({}); }
struct TArgs_float4pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4pl() { return TGenericExec<TPgDirectFunc<&float4pl>, true, true, TArgs_float4pl_Policy>({}); }
struct TArgs_float4recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4recv() { return TGenericExec<TPgDirectFunc<&float4recv>, true, true, TArgs_float4recv_Policy>({}); }
struct TArgs_float4send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4send() { return TGenericExec<TPgDirectFunc<&float4send>, true, false, TArgs_float4send_Policy>({}); }
struct TArgs_float4smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float4smaller() { return TGenericExec<TPgDirectFunc<&float4smaller>, true, true, TArgs_float4smaller_Policy>({}); }
struct TArgs_float4um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4um() { return TGenericExec<TPgDirectFunc<&float4um>, true, true, TArgs_float4um_Policy>({}); }
struct TArgs_float4up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4up() { return TGenericExec<TPgDirectFunc<&float4up>, true, true, TArgs_float4up_Policy>({}); }
struct TArgs_float84div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84div() { return TGenericExec<TPgDirectFunc<&float84div>, true, true, TArgs_float84div_Policy>({}); }
struct TArgs_float84eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84eq() { return TGenericExec<TPgDirectFunc<&float84eq>, true, true, TArgs_float84eq_Policy>({}); }
struct TArgs_float84ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84ge() { return TGenericExec<TPgDirectFunc<&float84ge>, true, true, TArgs_float84ge_Policy>({}); }
struct TArgs_float84gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84gt() { return TGenericExec<TPgDirectFunc<&float84gt>, true, true, TArgs_float84gt_Policy>({}); }
struct TArgs_float84le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84le() { return TGenericExec<TPgDirectFunc<&float84le>, true, true, TArgs_float84le_Policy>({}); }
struct TArgs_float84lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84lt() { return TGenericExec<TPgDirectFunc<&float84lt>, true, true, TArgs_float84lt_Policy>({}); }
struct TArgs_float84mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84mi() { return TGenericExec<TPgDirectFunc<&float84mi>, true, true, TArgs_float84mi_Policy>({}); }
struct TArgs_float84mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84mul() { return TGenericExec<TPgDirectFunc<&float84mul>, true, true, TArgs_float84mul_Policy>({}); }
struct TArgs_float84ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84ne() { return TGenericExec<TPgDirectFunc<&float84ne>, true, true, TArgs_float84ne_Policy>({}); }
struct TArgs_float84pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float84pl() { return TGenericExec<TPgDirectFunc<&float84pl>, true, true, TArgs_float84pl_Policy>({}); }
struct TArgs_float8_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_float8_accum() { return TGenericExec<TPgDirectFunc<&float8_accum>, true, false, TArgs_float8_accum_Policy>({}); }
struct TArgs_float8_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_avg() { return TGenericExec<TPgDirectFunc<&float8_avg>, true, true, TArgs_float8_avg_Policy>({}); }
struct TArgs_float8_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_float8_combine() { return TGenericExec<TPgDirectFunc<&float8_combine>, true, false, TArgs_float8_combine_Policy>({}); }
struct TArgs_float8_corr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_corr() { return TGenericExec<TPgDirectFunc<&float8_corr>, true, true, TArgs_float8_corr_Policy>({}); }
struct TArgs_float8_covar_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_covar_pop() { return TGenericExec<TPgDirectFunc<&float8_covar_pop>, true, true, TArgs_float8_covar_pop_Policy>({}); }
struct TArgs_float8_covar_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_covar_samp() { return TGenericExec<TPgDirectFunc<&float8_covar_samp>, true, true, TArgs_float8_covar_samp_Policy>({}); }
struct TArgs_float8_regr_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_float8_regr_accum() { return TGenericExec<TPgDirectFunc<&float8_regr_accum>, true, false, TArgs_float8_regr_accum_Policy>({}); }
struct TArgs_float8_regr_avgx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_avgx() { return TGenericExec<TPgDirectFunc<&float8_regr_avgx>, true, true, TArgs_float8_regr_avgx_Policy>({}); }
struct TArgs_float8_regr_avgy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_avgy() { return TGenericExec<TPgDirectFunc<&float8_regr_avgy>, true, true, TArgs_float8_regr_avgy_Policy>({}); }
struct TArgs_float8_regr_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_float8_regr_combine() { return TGenericExec<TPgDirectFunc<&float8_regr_combine>, true, false, TArgs_float8_regr_combine_Policy>({}); }
struct TArgs_float8_regr_intercept_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_intercept() { return TGenericExec<TPgDirectFunc<&float8_regr_intercept>, true, true, TArgs_float8_regr_intercept_Policy>({}); }
struct TArgs_float8_regr_r2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_r2() { return TGenericExec<TPgDirectFunc<&float8_regr_r2>, true, true, TArgs_float8_regr_r2_Policy>({}); }
struct TArgs_float8_regr_slope_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_slope() { return TGenericExec<TPgDirectFunc<&float8_regr_slope>, true, true, TArgs_float8_regr_slope_Policy>({}); }
struct TArgs_float8_regr_sxx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_sxx() { return TGenericExec<TPgDirectFunc<&float8_regr_sxx>, true, true, TArgs_float8_regr_sxx_Policy>({}); }
struct TArgs_float8_regr_sxy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_sxy() { return TGenericExec<TPgDirectFunc<&float8_regr_sxy>, true, true, TArgs_float8_regr_sxy_Policy>({}); }
struct TArgs_float8_regr_syy_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_regr_syy() { return TGenericExec<TPgDirectFunc<&float8_regr_syy>, true, true, TArgs_float8_regr_syy_Policy>({}); }
struct TArgs_float8_stddev_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_stddev_pop() { return TGenericExec<TPgDirectFunc<&float8_stddev_pop>, true, true, TArgs_float8_stddev_pop_Policy>({}); }
struct TArgs_float8_stddev_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_stddev_samp() { return TGenericExec<TPgDirectFunc<&float8_stddev_samp>, true, true, TArgs_float8_stddev_samp_Policy>({}); }
struct TArgs_float8_var_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_var_pop() { return TGenericExec<TPgDirectFunc<&float8_var_pop>, true, true, TArgs_float8_var_pop_Policy>({}); }
struct TArgs_float8_var_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8_var_samp() { return TGenericExec<TPgDirectFunc<&float8_var_samp>, true, true, TArgs_float8_var_samp_Policy>({}); }
struct TArgs_float8abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8abs() { return TGenericExec<TPgDirectFunc<&float8abs>, true, true, TArgs_float8abs_Policy>({}); }
struct TArgs_float8div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8div() { return TGenericExec<TPgDirectFunc<&float8div>, true, true, TArgs_float8div_Policy>({}); }
struct TArgs_float8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8eq() { return TGenericExec<TPgDirectFunc<&float8eq>, true, true, TArgs_float8eq_Policy>({}); }
struct TArgs_float8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8ge() { return TGenericExec<TPgDirectFunc<&float8ge>, true, true, TArgs_float8ge_Policy>({}); }
struct TArgs_float8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8gt() { return TGenericExec<TPgDirectFunc<&float8gt>, true, true, TArgs_float8gt_Policy>({}); }
struct TArgs_float8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_float8in() { return TGenericExec<TPgDirectFunc<&float8in>, true, true, TArgs_float8in_Policy>({}); }
struct TArgs_float8larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8larger() { return TGenericExec<TPgDirectFunc<&float8larger>, true, true, TArgs_float8larger_Policy>({}); }
struct TArgs_float8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8le() { return TGenericExec<TPgDirectFunc<&float8le>, true, true, TArgs_float8le_Policy>({}); }
struct TArgs_float8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8lt() { return TGenericExec<TPgDirectFunc<&float8lt>, true, true, TArgs_float8lt_Policy>({}); }
struct TArgs_float8mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8mi() { return TGenericExec<TPgDirectFunc<&float8mi>, true, true, TArgs_float8mi_Policy>({}); }
struct TArgs_float8mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8mul() { return TGenericExec<TPgDirectFunc<&float8mul>, true, true, TArgs_float8mul_Policy>({}); }
struct TArgs_float8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8ne() { return TGenericExec<TPgDirectFunc<&float8ne>, true, true, TArgs_float8ne_Policy>({}); }
struct TArgs_float8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8out() { return TGenericExec<TPgDirectFunc<&float8out>, true, false, TArgs_float8out_Policy>({}); }
struct TArgs_float8pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8pl() { return TGenericExec<TPgDirectFunc<&float8pl>, true, true, TArgs_float8pl_Policy>({}); }
struct TArgs_float8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8recv() { return TGenericExec<TPgDirectFunc<&float8recv>, true, true, TArgs_float8recv_Policy>({}); }
struct TArgs_float8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8send() { return TGenericExec<TPgDirectFunc<&float8send>, true, false, TArgs_float8send_Policy>({}); }
struct TArgs_float8smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_float8smaller() { return TGenericExec<TPgDirectFunc<&float8smaller>, true, true, TArgs_float8smaller_Policy>({}); }
struct TArgs_float8um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8um() { return TGenericExec<TPgDirectFunc<&float8um>, true, true, TArgs_float8um_Policy>({}); }
struct TArgs_float8up_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8up() { return TGenericExec<TPgDirectFunc<&float8up>, true, true, TArgs_float8up_Policy>({}); }
struct TArgs_ftod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftod() { return TGenericExec<TPgDirectFunc<&ftod>, true, true, TArgs_ftod_Policy>({}); }
struct TArgs_ftoi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi2() { return TGenericExec<TPgDirectFunc<&ftoi2>, true, true, TArgs_ftoi2_Policy>({}); }
struct TArgs_ftoi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi4() { return TGenericExec<TPgDirectFunc<&ftoi4>, true, true, TArgs_ftoi4_Policy>({}); }
struct TArgs_gin_clean_pending_list_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gin_clean_pending_list() { return TGenericExec<TPgDirectFunc<&gin_clean_pending_list>, true, true, TArgs_gin_clean_pending_list_Policy>({}); }
struct TArgs_ginhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ginhandler() { return TGenericExec<TPgDirectFunc<&ginhandler>, true, true, TArgs_ginhandler_Policy>({}); }
struct TArgs_gist_box_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_box_consistent() { return TGenericExec<TPgDirectFunc<&gist_box_consistent>, true, true, TArgs_gist_box_consistent_Policy>({}); }
struct TArgs_gist_box_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_box_distance() { return TGenericExec<TPgDirectFunc<&gist_box_distance>, true, true, TArgs_gist_box_distance_Policy>({}); }
struct TArgs_gist_box_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gist_box_penalty() { return TGenericExec<TPgDirectFunc<&gist_box_penalty>, true, true, TArgs_gist_box_penalty_Policy>({}); }
struct TArgs_gist_box_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gist_box_picksplit() { return TGenericExec<TPgDirectFunc<&gist_box_picksplit>, true, true, TArgs_gist_box_picksplit_Policy>({}); }
struct TArgs_gist_box_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_gist_box_same() { return TGenericExec<TPgDirectFunc<&gist_box_same>, true, true, TArgs_gist_box_same_Policy>({}); }
struct TArgs_gist_box_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gist_box_union() { return TGenericExec<TPgDirectFunc<&gist_box_union>, true, false, TArgs_gist_box_union_Policy>({}); }
struct TArgs_gist_circle_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_circle_compress() { return TGenericExec<TPgDirectFunc<&gist_circle_compress>, true, true, TArgs_gist_circle_compress_Policy>({}); }
struct TArgs_gist_circle_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_circle_consistent() { return TGenericExec<TPgDirectFunc<&gist_circle_consistent>, true, true, TArgs_gist_circle_consistent_Policy>({}); }
struct TArgs_gist_circle_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_circle_distance() { return TGenericExec<TPgDirectFunc<&gist_circle_distance>, true, true, TArgs_gist_circle_distance_Policy>({}); }
struct TArgs_gist_point_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_compress() { return TGenericExec<TPgDirectFunc<&gist_point_compress>, true, true, TArgs_gist_point_compress_Policy>({}); }
struct TArgs_gist_point_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_point_consistent() { return TGenericExec<TPgDirectFunc<&gist_point_consistent>, true, true, TArgs_gist_point_consistent_Policy>({}); }
struct TArgs_gist_point_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_point_distance() { return TGenericExec<TPgDirectFunc<&gist_point_distance>, true, true, TArgs_gist_point_distance_Policy>({}); }
struct TArgs_gist_point_fetch_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_fetch() { return TGenericExec<TPgDirectFunc<&gist_point_fetch>, true, true, TArgs_gist_point_fetch_Policy>({}); }
struct TArgs_gist_point_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_point_sortsupport() { return TGenericExec<TPgDirectFunc<&gist_point_sortsupport>, true, true, TArgs_gist_point_sortsupport_Policy>({}); }
struct TArgs_gist_poly_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gist_poly_compress() { return TGenericExec<TPgDirectFunc<&gist_poly_compress>, true, true, TArgs_gist_poly_compress_Policy>({}); }
struct TArgs_gist_poly_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_poly_consistent() { return TGenericExec<TPgDirectFunc<&gist_poly_consistent>, true, true, TArgs_gist_poly_consistent_Policy>({}); }
struct TArgs_gist_poly_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gist_poly_distance() { return TGenericExec<TPgDirectFunc<&gist_poly_distance>, true, true, TArgs_gist_poly_distance_Policy>({}); }
struct TArgs_hash_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_multirange() { return TGenericExec<TPgDirectFunc<&hash_multirange>, true, true, TArgs_hash_multirange_Policy>({}); }
struct TArgs_hash_multirange_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_multirange_extended() { return TGenericExec<TPgDirectFunc<&hash_multirange_extended>, true, true, TArgs_hash_multirange_extended_Policy>({}); }
struct TArgs_hash_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_record() { return TGenericExec<TPgDirectFunc<&hash_record>, true, true, TArgs_hash_record_Policy>({}); }
struct TArgs_hash_record_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_record_extended() { return TGenericExec<TPgDirectFunc<&hash_record_extended>, true, true, TArgs_hash_record_extended_Policy>({}); }
struct TArgs_hashbpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashbpchar() { return TGenericExec<TPgDirectFunc<&hashbpchar>, true, true, TArgs_hashbpchar_Policy>({}); }
struct TArgs_hashbpcharextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashbpcharextended() { return TGenericExec<TPgDirectFunc<&hashbpcharextended>, true, true, TArgs_hashbpcharextended_Policy>({}); }
struct TArgs_hashhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_hashhandler() { return TGenericExec<TPgDirectFunc<&hashhandler>, true, true, TArgs_hashhandler_Policy>({}); }
struct TArgs_hashmacaddr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashmacaddr() { return TGenericExec<TPgDirectFunc<&hashmacaddr>, true, true, TArgs_hashmacaddr_Policy>({}); }
struct TArgs_hashmacaddrextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashmacaddrextended() { return TGenericExec<TPgDirectFunc<&hashmacaddrextended>, true, true, TArgs_hashmacaddrextended_Policy>({}); }
struct TArgs_i2tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2tod() { return TGenericExec<TPgDirectFunc<&i2tod>, true, true, TArgs_i2tod_Policy>({}); }
struct TArgs_i2tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2tof() { return TGenericExec<TPgDirectFunc<&i2tof>, true, true, TArgs_i2tof_Policy>({}); }
struct TArgs_i4tod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tod() { return TGenericExec<TPgDirectFunc<&i4tod>, true, true, TArgs_i4tod_Policy>({}); }
struct TArgs_i4tof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4tof() { return TGenericExec<TPgDirectFunc<&i4tof>, true, true, TArgs_i4tof_Policy>({}); }
struct TArgs_in_range_float4_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_float4_float8() { return TGenericExec<TPgDirectFunc<&in_range_float4_float8>, true, true, TArgs_in_range_float4_float8_Policy>({}); }
struct TArgs_in_range_float8_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_float8_float8() { return TGenericExec<TPgDirectFunc<&in_range_float8_float8>, true, true, TArgs_in_range_float8_float8_Policy>({}); }
struct TArgs_index_am_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_index_am_handler_in() { return TGenericExec<TPgDirectFunc<&index_am_handler_in>, false, true, TArgs_index_am_handler_in_Policy>({}); }
struct TArgs_index_am_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_index_am_handler_out() { return TGenericExec<TPgDirectFunc<&index_am_handler_out>, true, false, TArgs_index_am_handler_out_Policy>({}); }
struct TArgs_internal_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_internal_in() { return TGenericExec<TPgDirectFunc<&internal_in>, false, true, TArgs_internal_in_Policy>({}); }
struct TArgs_internal_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_internal_out() { return TGenericExec<TPgDirectFunc<&internal_out>, true, false, TArgs_internal_out_Policy>({}); }
struct TArgs_language_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_language_handler_in() { return TGenericExec<TPgDirectFunc<&language_handler_in>, false, true, TArgs_language_handler_in_Policy>({}); }
struct TArgs_language_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_language_handler_out() { return TGenericExec<TPgDirectFunc<&language_handler_out>, true, false, TArgs_language_handler_out_Policy>({}); }
struct TArgs_lastval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_lastval() { return TGenericExec<TPgDirectFunc<&lastval>, true, true, TArgs_lastval_Policy>({}); }
struct TArgs_macaddr_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_and() { return TGenericExec<TPgDirectFunc<&macaddr_and>, true, false, TArgs_macaddr_and_Policy>({}); }
struct TArgs_macaddr_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_cmp() { return TGenericExec<TPgDirectFunc<&macaddr_cmp>, true, true, TArgs_macaddr_cmp_Policy>({}); }
struct TArgs_macaddr_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_eq() { return TGenericExec<TPgDirectFunc<&macaddr_eq>, true, true, TArgs_macaddr_eq_Policy>({}); }
struct TArgs_macaddr_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_ge() { return TGenericExec<TPgDirectFunc<&macaddr_ge>, true, true, TArgs_macaddr_ge_Policy>({}); }
struct TArgs_macaddr_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_gt() { return TGenericExec<TPgDirectFunc<&macaddr_gt>, true, true, TArgs_macaddr_gt_Policy>({}); }
struct TArgs_macaddr_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_in() { return TGenericExec<TPgDirectFunc<&macaddr_in>, true, false, TArgs_macaddr_in_Policy>({}); }
struct TArgs_macaddr_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_le() { return TGenericExec<TPgDirectFunc<&macaddr_le>, true, true, TArgs_macaddr_le_Policy>({}); }
struct TArgs_macaddr_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_lt() { return TGenericExec<TPgDirectFunc<&macaddr_lt>, true, true, TArgs_macaddr_lt_Policy>({}); }
struct TArgs_macaddr_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_ne() { return TGenericExec<TPgDirectFunc<&macaddr_ne>, true, true, TArgs_macaddr_ne_Policy>({}); }
struct TArgs_macaddr_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_not() { return TGenericExec<TPgDirectFunc<&macaddr_not>, true, false, TArgs_macaddr_not_Policy>({}); }
struct TArgs_macaddr_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_or() { return TGenericExec<TPgDirectFunc<&macaddr_or>, true, false, TArgs_macaddr_or_Policy>({}); }
struct TArgs_macaddr_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_out() { return TGenericExec<TPgDirectFunc<&macaddr_out>, true, false, TArgs_macaddr_out_Policy>({}); }
struct TArgs_macaddr_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_macaddr_recv() { return TGenericExec<TPgDirectFunc<&macaddr_recv>, true, false, TArgs_macaddr_recv_Policy>({}); }
struct TArgs_macaddr_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_send() { return TGenericExec<TPgDirectFunc<&macaddr_send>, true, false, TArgs_macaddr_send_Policy>({}); }
struct TArgs_macaddr_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_macaddr_sortsupport() { return TGenericExec<TPgDirectFunc<&macaddr_sortsupport>, true, true, TArgs_macaddr_sortsupport_Policy>({}); }
struct TArgs_macaddr_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_trunc() { return TGenericExec<TPgDirectFunc<&macaddr_trunc>, true, false, TArgs_macaddr_trunc_Policy>({}); }
struct TArgs_mode_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_mode_final() { return TGenericExec<TPgDirectFunc<&mode_final>, false, true, TArgs_mode_final_Policy>({}); }
struct TArgs_multirange_adjacent_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_adjacent_multirange() { return TGenericExec<TPgDirectFunc<&multirange_adjacent_multirange>, true, true, TArgs_multirange_adjacent_multirange_Policy>({}); }
struct TArgs_multirange_adjacent_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_adjacent_range() { return TGenericExec<TPgDirectFunc<&multirange_adjacent_range>, true, true, TArgs_multirange_adjacent_range_Policy>({}); }
struct TArgs_multirange_after_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_after_multirange() { return TGenericExec<TPgDirectFunc<&multirange_after_multirange>, true, true, TArgs_multirange_after_multirange_Policy>({}); }
struct TArgs_multirange_after_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_after_range() { return TGenericExec<TPgDirectFunc<&multirange_after_range>, true, true, TArgs_multirange_after_range_Policy>({}); }
struct TArgs_multirange_before_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_before_multirange() { return TGenericExec<TPgDirectFunc<&multirange_before_multirange>, true, true, TArgs_multirange_before_multirange_Policy>({}); }
struct TArgs_multirange_before_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_before_range() { return TGenericExec<TPgDirectFunc<&multirange_before_range>, true, true, TArgs_multirange_before_range_Policy>({}); }
struct TArgs_multirange_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_cmp() { return TGenericExec<TPgDirectFunc<&multirange_cmp>, true, true, TArgs_multirange_cmp_Policy>({}); }
struct TArgs_multirange_constructor0_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_multirange_constructor0() { return TGenericExec<TPgDirectFunc<&multirange_constructor0>, true, false, TArgs_multirange_constructor0_Policy>({}); }
struct TArgs_multirange_constructor1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_constructor1() { return TGenericExec<TPgDirectFunc<&multirange_constructor1>, true, false, TArgs_multirange_constructor1_Policy>({}); }
struct TArgs_multirange_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contained_by_multirange() { return TGenericExec<TPgDirectFunc<&multirange_contained_by_multirange>, true, true, TArgs_multirange_contained_by_multirange_Policy>({}); }
struct TArgs_multirange_contained_by_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contained_by_range() { return TGenericExec<TPgDirectFunc<&multirange_contained_by_range>, true, true, TArgs_multirange_contained_by_range_Policy>({}); }
struct TArgs_multirange_contains_elem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_multirange_contains_elem() { return TGenericExec<TPgDirectFunc<&multirange_contains_elem>, true, true, TArgs_multirange_contains_elem_Policy>({}); }
struct TArgs_multirange_contains_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contains_multirange() { return TGenericExec<TPgDirectFunc<&multirange_contains_multirange>, true, true, TArgs_multirange_contains_multirange_Policy>({}); }
struct TArgs_multirange_contains_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_contains_range() { return TGenericExec<TPgDirectFunc<&multirange_contains_range>, true, true, TArgs_multirange_contains_range_Policy>({}); }
struct TArgs_multirange_empty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_empty() { return TGenericExec<TPgDirectFunc<&multirange_empty>, true, true, TArgs_multirange_empty_Policy>({}); }
struct TArgs_multirange_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_eq() { return TGenericExec<TPgDirectFunc<&multirange_eq>, true, true, TArgs_multirange_eq_Policy>({}); }
struct TArgs_multirange_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_ge() { return TGenericExec<TPgDirectFunc<&multirange_ge>, true, true, TArgs_multirange_ge_Policy>({}); }
struct TArgs_multirange_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_gt() { return TGenericExec<TPgDirectFunc<&multirange_gt>, true, true, TArgs_multirange_gt_Policy>({}); }
struct TArgs_multirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_multirange_in() { return TGenericExec<TPgDirectFunc<&multirange_in>, true, false, TArgs_multirange_in_Policy>({}); }
struct TArgs_multirange_intersect_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_intersect() { return TGenericExec<TPgDirectFunc<&multirange_intersect>, true, false, TArgs_multirange_intersect_Policy>({}); }
struct TArgs_multirange_intersect_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_intersect_agg_transfn() { return TGenericExec<TPgDirectFunc<&multirange_intersect_agg_transfn>, true, false, TArgs_multirange_intersect_agg_transfn_Policy>({}); }
struct TArgs_multirange_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_le() { return TGenericExec<TPgDirectFunc<&multirange_le>, true, true, TArgs_multirange_le_Policy>({}); }
struct TArgs_multirange_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower() { return TGenericExec<TPgDirectFunc<&multirange_lower>, true, true, TArgs_multirange_lower_Policy>({}); }
struct TArgs_multirange_lower_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower_inc() { return TGenericExec<TPgDirectFunc<&multirange_lower_inc>, true, true, TArgs_multirange_lower_inc_Policy>({}); }
struct TArgs_multirange_lower_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_lower_inf() { return TGenericExec<TPgDirectFunc<&multirange_lower_inf>, true, true, TArgs_multirange_lower_inf_Policy>({}); }
struct TArgs_multirange_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_lt() { return TGenericExec<TPgDirectFunc<&multirange_lt>, true, true, TArgs_multirange_lt_Policy>({}); }
struct TArgs_multirange_minus_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_minus() { return TGenericExec<TPgDirectFunc<&multirange_minus>, true, false, TArgs_multirange_minus_Policy>({}); }
struct TArgs_multirange_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_ne() { return TGenericExec<TPgDirectFunc<&multirange_ne>, true, true, TArgs_multirange_ne_Policy>({}); }
struct TArgs_multirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_out() { return TGenericExec<TPgDirectFunc<&multirange_out>, true, false, TArgs_multirange_out_Policy>({}); }
struct TArgs_multirange_overlaps_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overlaps_multirange() { return TGenericExec<TPgDirectFunc<&multirange_overlaps_multirange>, true, true, TArgs_multirange_overlaps_multirange_Policy>({}); }
struct TArgs_multirange_overlaps_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overlaps_range() { return TGenericExec<TPgDirectFunc<&multirange_overlaps_range>, true, true, TArgs_multirange_overlaps_range_Policy>({}); }
struct TArgs_multirange_overleft_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overleft_multirange() { return TGenericExec<TPgDirectFunc<&multirange_overleft_multirange>, true, true, TArgs_multirange_overleft_multirange_Policy>({}); }
struct TArgs_multirange_overleft_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overleft_range() { return TGenericExec<TPgDirectFunc<&multirange_overleft_range>, true, true, TArgs_multirange_overleft_range_Policy>({}); }
struct TArgs_multirange_overright_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overright_multirange() { return TGenericExec<TPgDirectFunc<&multirange_overright_multirange>, true, true, TArgs_multirange_overright_multirange_Policy>({}); }
struct TArgs_multirange_overright_range_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_overright_range() { return TGenericExec<TPgDirectFunc<&multirange_overright_range>, true, true, TArgs_multirange_overright_range_Policy>({}); }
struct TArgs_multirange_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_multirange_recv() { return TGenericExec<TPgDirectFunc<&multirange_recv>, true, false, TArgs_multirange_recv_Policy>({}); }
struct TArgs_multirange_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_send() { return TGenericExec<TPgDirectFunc<&multirange_send>, true, false, TArgs_multirange_send_Policy>({}); }
struct TArgs_multirange_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_multirange_union() { return TGenericExec<TPgDirectFunc<&multirange_union>, true, false, TArgs_multirange_union_Policy>({}); }
struct TArgs_multirange_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper() { return TGenericExec<TPgDirectFunc<&multirange_upper>, true, true, TArgs_multirange_upper_Policy>({}); }
struct TArgs_multirange_upper_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper_inc() { return TGenericExec<TPgDirectFunc<&multirange_upper_inc>, true, true, TArgs_multirange_upper_inc_Policy>({}); }
struct TArgs_multirange_upper_inf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_multirange_upper_inf() { return TGenericExec<TPgDirectFunc<&multirange_upper_inf>, true, true, TArgs_multirange_upper_inf_Policy>({}); }
struct TArgs_multirangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_multirangesel() { return TGenericExec<TPgDirectFunc<&multirangesel>, true, true, TArgs_multirangesel_Policy>({}); }
struct TArgs_mxid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_mxid_age() { return TGenericExec<TPgDirectFunc<&mxid_age>, true, true, TArgs_mxid_age_Policy>({}); }
struct TArgs_name_bpchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_name_bpchar() { return TGenericExec<TPgDirectFunc<&name_bpchar>, true, false, TArgs_name_bpchar_Policy>({}); }
struct TArgs_nameconcatoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_nameconcatoid() { return TGenericExec<TPgDirectFunc<&nameconcatoid>, true, false, TArgs_nameconcatoid_Policy>({}); }
struct TArgs_nameeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_nameeq() { return TGenericExec<TPgDirectFunc<&nameeq>, true, true, TArgs_nameeq_Policy>({}); }
struct TArgs_namege_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namege() { return TGenericExec<TPgDirectFunc<&namege>, true, true, TArgs_namege_Policy>({}); }
struct TArgs_namegt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namegt() { return TGenericExec<TPgDirectFunc<&namegt>, true, true, TArgs_namegt_Policy>({}); }
struct TArgs_namein_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_namein() { return TGenericExec<TPgDirectFunc<&namein>, true, false, TArgs_namein_Policy>({}); }
struct TArgs_namele_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namele() { return TGenericExec<TPgDirectFunc<&namele>, true, true, TArgs_namele_Policy>({}); }
struct TArgs_namelt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelt() { return TGenericExec<TPgDirectFunc<&namelt>, true, true, TArgs_namelt_Policy>({}); }
struct TArgs_namene_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namene() { return TGenericExec<TPgDirectFunc<&namene>, true, true, TArgs_namene_Policy>({}); }
struct TArgs_nameout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_nameout() { return TGenericExec<TPgDirectFunc<&nameout>, true, false, TArgs_nameout_Policy>({}); }
struct TArgs_namerecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_namerecv() { return TGenericExec<TPgDirectFunc<&namerecv>, true, false, TArgs_namerecv_Policy>({}); }
struct TArgs_namesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_namesend() { return TGenericExec<TPgDirectFunc<&namesend>, true, false, TArgs_namesend_Policy>({}); }
struct TArgs_networkjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_networkjoinsel() { return TGenericExec<TPgDirectFunc<&networkjoinsel>, true, true, TArgs_networkjoinsel_Policy>({}); }
struct TArgs_networksel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_networksel() { return TGenericExec<TPgDirectFunc<&networksel>, true, true, TArgs_networksel_Policy>({}); }
struct TArgs_nextval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_nextval_oid() { return TGenericExec<TPgDirectFunc<&nextval_oid>, true, true, TArgs_nextval_oid_Policy>({}); }
struct TArgs_ordered_set_transition_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_ordered_set_transition() { return TGenericExec<TPgDirectFunc<&ordered_set_transition>, false, true, TArgs_ordered_set_transition_Policy>({}); }
struct TArgs_percentile_cont_float8_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_percentile_cont_float8_final() { return TGenericExec<TPgDirectFunc<&percentile_cont_float8_final>, false, true, TArgs_percentile_cont_float8_final_Policy>({}); }
struct TArgs_percentile_cont_float8_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_percentile_cont_float8_multi_final() { return TGenericExec<TPgDirectFunc<&percentile_cont_float8_multi_final>, false, false, TArgs_percentile_cont_float8_multi_final_Policy>({}); }
struct TArgs_percentile_cont_interval_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_percentile_cont_interval_final() { return TGenericExec<TPgDirectFunc<&percentile_cont_interval_final>, false, false, TArgs_percentile_cont_interval_final_Policy>({}); }
struct TArgs_percentile_cont_interval_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_percentile_cont_interval_multi_final() { return TGenericExec<TPgDirectFunc<&percentile_cont_interval_multi_final>, false, false, TArgs_percentile_cont_interval_multi_final_Policy>({}); }
struct TArgs_percentile_disc_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_percentile_disc_final() { return TGenericExec<TPgDirectFunc<&percentile_disc_final>, false, true, TArgs_percentile_disc_final_Policy>({}); }
struct TArgs_percentile_disc_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_percentile_disc_multi_final() { return TGenericExec<TPgDirectFunc<&percentile_disc_multi_final>, false, false, TArgs_percentile_disc_multi_final_Policy>({}); }
struct TArgs_pg_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backend_pid() { return TGenericExec<TPgDirectFunc<&pg_backend_pid>, true, true, TArgs_pg_backend_pid_Policy>({}); }
struct TArgs_pg_backup_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backup_start_time() { return TGenericExec<TPgDirectFunc<&pg_backup_start_time>, true, true, TArgs_pg_backup_start_time_Policy>({}); }
struct TArgs_pg_collation_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_is_visible() { return TGenericExec<TPgDirectFunc<&pg_collation_is_visible>, true, true, TArgs_pg_collation_is_visible_Policy>({}); }
struct TArgs_pg_control_checkpoint_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_checkpoint() { return TGenericExec<TPgDirectFunc<&pg_control_checkpoint>, true, false, TArgs_pg_control_checkpoint_Policy>({}); }
struct TArgs_pg_control_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_init() { return TGenericExec<TPgDirectFunc<&pg_control_init>, true, false, TArgs_pg_control_init_Policy>({}); }
struct TArgs_pg_control_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_recovery() { return TGenericExec<TPgDirectFunc<&pg_control_recovery>, true, false, TArgs_pg_control_recovery_Policy>({}); }
struct TArgs_pg_control_system_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_control_system() { return TGenericExec<TPgDirectFunc<&pg_control_system>, true, false, TArgs_pg_control_system_Policy>({}); }
struct TArgs_pg_conversion_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_conversion_is_visible() { return TGenericExec<TPgDirectFunc<&pg_conversion_is_visible>, true, true, TArgs_pg_conversion_is_visible_Policy>({}); }
struct TArgs_pg_create_restore_point_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_create_restore_point() { return TGenericExec<TPgDirectFunc<&pg_create_restore_point>, true, true, TArgs_pg_create_restore_point_Policy>({}); }
struct TArgs_pg_current_wal_flush_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_flush_lsn() { return TGenericExec<TPgDirectFunc<&pg_current_wal_flush_lsn>, true, true, TArgs_pg_current_wal_flush_lsn_Policy>({}); }
struct TArgs_pg_current_wal_insert_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_insert_lsn() { return TGenericExec<TPgDirectFunc<&pg_current_wal_insert_lsn>, true, true, TArgs_pg_current_wal_insert_lsn_Policy>({}); }
struct TArgs_pg_current_wal_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_wal_lsn() { return TGenericExec<TPgDirectFunc<&pg_current_wal_lsn>, true, true, TArgs_pg_current_wal_lsn_Policy>({}); }
struct TArgs_pg_database_size_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_database_size_name() { return TGenericExec<TPgDirectFunc<&pg_database_size_name>, true, true, TArgs_pg_database_size_name_Policy>({}); }
struct TArgs_pg_database_size_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_database_size_oid() { return TGenericExec<TPgDirectFunc<&pg_database_size_oid>, true, true, TArgs_pg_database_size_oid_Policy>({}); }
struct TArgs_pg_ddl_command_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_ddl_command_in() { return TGenericExec<TPgDirectFunc<&pg_ddl_command_in>, true, true, TArgs_pg_ddl_command_in_Policy>({}); }
struct TArgs_pg_ddl_command_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_out() { return TGenericExec<TPgDirectFunc<&pg_ddl_command_out>, true, false, TArgs_pg_ddl_command_out_Policy>({}); }
struct TArgs_pg_ddl_command_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_recv() { return TGenericExec<TPgDirectFunc<&pg_ddl_command_recv>, true, true, TArgs_pg_ddl_command_recv_Policy>({}); }
struct TArgs_pg_ddl_command_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_send() { return TGenericExec<TPgDirectFunc<&pg_ddl_command_send>, true, false, TArgs_pg_ddl_command_send_Policy>({}); }
struct TArgs_pg_extension_config_dump_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_extension_config_dump() { return TGenericExec<TPgDirectFunc<&pg_extension_config_dump>, true, true, TArgs_pg_extension_config_dump_Policy>({}); }
struct TArgs_pg_filenode_relation_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_filenode_relation() { return TGenericExec<TPgDirectFunc<&pg_filenode_relation>, true, true, TArgs_pg_filenode_relation_Policy>({}); }
struct TArgs_pg_function_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_function_is_visible() { return TGenericExec<TPgDirectFunc<&pg_function_is_visible>, true, true, TArgs_pg_function_is_visible_Policy>({}); }
struct TArgs_pg_get_constraintdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_constraintdef() { return TGenericExec<TPgDirectFunc<&pg_get_constraintdef>, true, false, TArgs_pg_get_constraintdef_Policy>({}); }
struct TArgs_pg_get_constraintdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_constraintdef_ext() { return TGenericExec<TPgDirectFunc<&pg_get_constraintdef_ext>, true, false, TArgs_pg_get_constraintdef_ext_Policy>({}); }
struct TArgs_pg_get_expr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_get_expr() { return TGenericExec<TPgDirectFunc<&pg_get_expr>, true, false, TArgs_pg_get_expr_Policy>({}); }
struct TArgs_pg_get_expr_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_get_expr_ext() { return TGenericExec<TPgDirectFunc<&pg_get_expr_ext>, true, false, TArgs_pg_get_expr_ext_Policy>({}); }
struct TArgs_pg_get_function_arg_default_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_function_arg_default() { return TGenericExec<TPgDirectFunc<&pg_get_function_arg_default>, true, false, TArgs_pg_get_function_arg_default_Policy>({}); }
struct TArgs_pg_get_function_arguments_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_arguments() { return TGenericExec<TPgDirectFunc<&pg_get_function_arguments>, true, false, TArgs_pg_get_function_arguments_Policy>({}); }
struct TArgs_pg_get_function_identity_arguments_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_identity_arguments() { return TGenericExec<TPgDirectFunc<&pg_get_function_identity_arguments>, true, false, TArgs_pg_get_function_identity_arguments_Policy>({}); }
struct TArgs_pg_get_function_result_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_result() { return TGenericExec<TPgDirectFunc<&pg_get_function_result>, true, false, TArgs_pg_get_function_result_Policy>({}); }
struct TArgs_pg_get_function_sqlbody_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_function_sqlbody() { return TGenericExec<TPgDirectFunc<&pg_get_function_sqlbody>, true, false, TArgs_pg_get_function_sqlbody_Policy>({}); }
struct TArgs_pg_get_functiondef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_functiondef() { return TGenericExec<TPgDirectFunc<&pg_get_functiondef>, true, false, TArgs_pg_get_functiondef_Policy>({}); }
struct TArgs_pg_get_indexdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_indexdef() { return TGenericExec<TPgDirectFunc<&pg_get_indexdef>, true, false, TArgs_pg_get_indexdef_Policy>({}); }
struct TArgs_pg_get_indexdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_get_indexdef_ext() { return TGenericExec<TPgDirectFunc<&pg_get_indexdef_ext>, true, false, TArgs_pg_get_indexdef_ext_Policy>({}); }
struct TArgs_pg_get_partition_constraintdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_partition_constraintdef() { return TGenericExec<TPgDirectFunc<&pg_get_partition_constraintdef>, true, false, TArgs_pg_get_partition_constraintdef_Policy>({}); }
struct TArgs_pg_get_partkeydef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_partkeydef() { return TGenericExec<TPgDirectFunc<&pg_get_partkeydef>, true, false, TArgs_pg_get_partkeydef_Policy>({}); }
struct TArgs_pg_get_ruledef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_ruledef() { return TGenericExec<TPgDirectFunc<&pg_get_ruledef>, true, false, TArgs_pg_get_ruledef_Policy>({}); }
struct TArgs_pg_get_ruledef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_ruledef_ext() { return TGenericExec<TPgDirectFunc<&pg_get_ruledef_ext>, true, false, TArgs_pg_get_ruledef_ext_Policy>({}); }
struct TArgs_pg_get_serial_sequence_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_pg_get_serial_sequence() { return TGenericExec<TPgDirectFunc<&pg_get_serial_sequence>, true, false, TArgs_pg_get_serial_sequence_Policy>({}); }
struct TArgs_pg_get_statisticsobjdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef() { return TGenericExec<TPgDirectFunc<&pg_get_statisticsobjdef>, true, false, TArgs_pg_get_statisticsobjdef_Policy>({}); }
struct TArgs_pg_get_statisticsobjdef_columns_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef_columns() { return TGenericExec<TPgDirectFunc<&pg_get_statisticsobjdef_columns>, true, false, TArgs_pg_get_statisticsobjdef_columns_Policy>({}); }
struct TArgs_pg_get_statisticsobjdef_expressions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_statisticsobjdef_expressions() { return TGenericExec<TPgDirectFunc<&pg_get_statisticsobjdef_expressions>, true, false, TArgs_pg_get_statisticsobjdef_expressions_Policy>({}); }
struct TArgs_pg_get_triggerdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_triggerdef() { return TGenericExec<TPgDirectFunc<&pg_get_triggerdef>, true, false, TArgs_pg_get_triggerdef_Policy>({}); }
struct TArgs_pg_get_triggerdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_triggerdef_ext() { return TGenericExec<TPgDirectFunc<&pg_get_triggerdef_ext>, true, false, TArgs_pg_get_triggerdef_ext_Policy>({}); }
struct TArgs_pg_get_userbyid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_userbyid() { return TGenericExec<TPgDirectFunc<&pg_get_userbyid>, true, false, TArgs_pg_get_userbyid_Policy>({}); }
struct TArgs_pg_get_viewdef_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_get_viewdef() { return TGenericExec<TPgDirectFunc<&pg_get_viewdef>, true, false, TArgs_pg_get_viewdef_Policy>({}); }
struct TArgs_pg_get_viewdef_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_viewdef_ext() { return TGenericExec<TPgDirectFunc<&pg_get_viewdef_ext>, true, false, TArgs_pg_get_viewdef_ext_Policy>({}); }
struct TArgs_pg_get_viewdef_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_get_viewdef_name() { return TGenericExec<TPgDirectFunc<&pg_get_viewdef_name>, true, false, TArgs_pg_get_viewdef_name_Policy>({}); }
struct TArgs_pg_get_viewdef_name_ext_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_get_viewdef_name_ext() { return TGenericExec<TPgDirectFunc<&pg_get_viewdef_name_ext>, true, false, TArgs_pg_get_viewdef_name_ext_Policy>({}); }
struct TArgs_pg_get_viewdef_wrap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_get_viewdef_wrap() { return TGenericExec<TPgDirectFunc<&pg_get_viewdef_wrap>, true, false, TArgs_pg_get_viewdef_wrap_Policy>({}); }
struct TArgs_pg_get_wal_replay_pause_state_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_get_wal_replay_pause_state() { return TGenericExec<TPgDirectFunc<&pg_get_wal_replay_pause_state>, true, false, TArgs_pg_get_wal_replay_pause_state_Policy>({}); }
struct TArgs_pg_index_column_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_pg_index_column_has_property() { return TGenericExec<TPgDirectFunc<&pg_index_column_has_property>, true, true, TArgs_pg_index_column_has_property_Policy>({}); }
struct TArgs_pg_index_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_index_has_property() { return TGenericExec<TPgDirectFunc<&pg_index_has_property>, true, true, TArgs_pg_index_has_property_Policy>({}); }
struct TArgs_pg_indexam_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_indexam_has_property() { return TGenericExec<TPgDirectFunc<&pg_indexam_has_property>, true, true, TArgs_pg_indexam_has_property_Policy>({}); }
struct TArgs_pg_indexam_progress_phasename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_indexam_progress_phasename() { return TGenericExec<TPgDirectFunc<&pg_indexam_progress_phasename>, true, false, TArgs_pg_indexam_progress_phasename_Policy>({}); }
struct TArgs_pg_indexes_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_indexes_size() { return TGenericExec<TPgDirectFunc<&pg_indexes_size>, true, true, TArgs_pg_indexes_size_Policy>({}); }
struct TArgs_pg_is_in_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_in_backup() { return TGenericExec<TPgDirectFunc<&pg_is_in_backup>, true, true, TArgs_pg_is_in_backup_Policy>({}); }
struct TArgs_pg_is_in_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_in_recovery() { return TGenericExec<TPgDirectFunc<&pg_is_in_recovery>, true, true, TArgs_pg_is_in_recovery_Policy>({}); }
struct TArgs_pg_is_other_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_is_other_temp_schema() { return TGenericExec<TPgDirectFunc<&pg_is_other_temp_schema>, true, true, TArgs_pg_is_other_temp_schema_Policy>({}); }
struct TArgs_pg_is_wal_replay_paused_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_wal_replay_paused() { return TGenericExec<TPgDirectFunc<&pg_is_wal_replay_paused>, true, true, TArgs_pg_is_wal_replay_paused_Policy>({}); }
struct TArgs_pg_jit_available_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_jit_available() { return TGenericExec<TPgDirectFunc<&pg_jit_available>, true, true, TArgs_pg_jit_available_Policy>({}); }
struct TArgs_pg_last_wal_receive_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_wal_receive_lsn() { return TGenericExec<TPgDirectFunc<&pg_last_wal_receive_lsn>, true, true, TArgs_pg_last_wal_receive_lsn_Policy>({}); }
struct TArgs_pg_last_wal_replay_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_wal_replay_lsn() { return TGenericExec<TPgDirectFunc<&pg_last_wal_replay_lsn>, true, true, TArgs_pg_last_wal_replay_lsn_Policy>({}); }
struct TArgs_pg_last_xact_replay_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_xact_replay_timestamp() { return TGenericExec<TPgDirectFunc<&pg_last_xact_replay_timestamp>, true, true, TArgs_pg_last_xact_replay_timestamp_Policy>({}); }
struct TArgs_pg_lsn_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_cmp() { return TGenericExec<TPgDirectFunc<&pg_lsn_cmp>, true, true, TArgs_pg_lsn_cmp_Policy>({}); }
struct TArgs_pg_lsn_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_eq() { return TGenericExec<TPgDirectFunc<&pg_lsn_eq>, true, true, TArgs_pg_lsn_eq_Policy>({}); }
struct TArgs_pg_lsn_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_ge() { return TGenericExec<TPgDirectFunc<&pg_lsn_ge>, true, true, TArgs_pg_lsn_ge_Policy>({}); }
struct TArgs_pg_lsn_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_gt() { return TGenericExec<TPgDirectFunc<&pg_lsn_gt>, true, true, TArgs_pg_lsn_gt_Policy>({}); }
struct TArgs_pg_lsn_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_hash() { return TGenericExec<TPgDirectFunc<&pg_lsn_hash>, true, true, TArgs_pg_lsn_hash_Policy>({}); }
struct TArgs_pg_lsn_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_hash_extended() { return TGenericExec<TPgDirectFunc<&pg_lsn_hash_extended>, true, true, TArgs_pg_lsn_hash_extended_Policy>({}); }
struct TArgs_pg_lsn_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_lsn_in() { return TGenericExec<TPgDirectFunc<&pg_lsn_in>, true, true, TArgs_pg_lsn_in_Policy>({}); }
struct TArgs_pg_lsn_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_larger() { return TGenericExec<TPgDirectFunc<&pg_lsn_larger>, true, true, TArgs_pg_lsn_larger_Policy>({}); }
struct TArgs_pg_lsn_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_le() { return TGenericExec<TPgDirectFunc<&pg_lsn_le>, true, true, TArgs_pg_lsn_le_Policy>({}); }
struct TArgs_pg_lsn_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_lt() { return TGenericExec<TPgDirectFunc<&pg_lsn_lt>, true, true, TArgs_pg_lsn_lt_Policy>({}); }
struct TArgs_pg_lsn_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_mi() { return TGenericExec<TPgDirectFunc<&pg_lsn_mi>, true, false, TArgs_pg_lsn_mi_Policy>({}); }
struct TArgs_pg_lsn_mii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_lsn_mii() { return TGenericExec<TPgDirectFunc<&pg_lsn_mii>, true, true, TArgs_pg_lsn_mii_Policy>({}); }
struct TArgs_pg_lsn_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_ne() { return TGenericExec<TPgDirectFunc<&pg_lsn_ne>, true, true, TArgs_pg_lsn_ne_Policy>({}); }
struct TArgs_pg_lsn_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_out() { return TGenericExec<TPgDirectFunc<&pg_lsn_out>, true, false, TArgs_pg_lsn_out_Policy>({}); }
struct TArgs_pg_lsn_pli_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_lsn_pli() { return TGenericExec<TPgDirectFunc<&pg_lsn_pli>, true, true, TArgs_pg_lsn_pli_Policy>({}); }
struct TArgs_pg_lsn_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_recv() { return TGenericExec<TPgDirectFunc<&pg_lsn_recv>, true, true, TArgs_pg_lsn_recv_Policy>({}); }
struct TArgs_pg_lsn_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_lsn_send() { return TGenericExec<TPgDirectFunc<&pg_lsn_send>, true, false, TArgs_pg_lsn_send_Policy>({}); }
struct TArgs_pg_lsn_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_lsn_smaller() { return TGenericExec<TPgDirectFunc<&pg_lsn_smaller>, true, true, TArgs_pg_lsn_smaller_Policy>({}); }
struct TArgs_pg_my_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_my_temp_schema() { return TGenericExec<TPgDirectFunc<&pg_my_temp_schema>, true, true, TArgs_pg_my_temp_schema_Policy>({}); }
struct TArgs_pg_node_tree_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_in() { return TGenericExec<TPgDirectFunc<&pg_node_tree_in>, true, false, TArgs_pg_node_tree_in_Policy>({}); }
struct TArgs_pg_node_tree_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_out() { return TGenericExec<TPgDirectFunc<&pg_node_tree_out>, true, false, TArgs_pg_node_tree_out_Policy>({}); }
struct TArgs_pg_node_tree_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_node_tree_recv() { return TGenericExec<TPgDirectFunc<&pg_node_tree_recv>, true, false, TArgs_pg_node_tree_recv_Policy>({}); }
struct TArgs_pg_node_tree_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_send() { return TGenericExec<TPgDirectFunc<&pg_node_tree_send>, true, false, TArgs_pg_node_tree_send_Policy>({}); }
struct TArgs_pg_opclass_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_opclass_is_visible() { return TGenericExec<TPgDirectFunc<&pg_opclass_is_visible>, true, true, TArgs_pg_opclass_is_visible_Policy>({}); }
struct TArgs_pg_operator_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_operator_is_visible() { return TGenericExec<TPgDirectFunc<&pg_operator_is_visible>, true, true, TArgs_pg_operator_is_visible_Policy>({}); }
struct TArgs_pg_opfamily_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_opfamily_is_visible() { return TGenericExec<TPgDirectFunc<&pg_opfamily_is_visible>, true, true, TArgs_pg_opfamily_is_visible_Policy>({}); }
struct TArgs_pg_promote_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_promote() { return TGenericExec<TPgDirectFunc<&pg_promote>, true, true, TArgs_pg_promote_Policy>({}); }
struct TArgs_pg_relation_filenode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_filenode() { return TGenericExec<TPgDirectFunc<&pg_relation_filenode>, true, true, TArgs_pg_relation_filenode_Policy>({}); }
struct TArgs_pg_relation_filepath_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_filepath() { return TGenericExec<TPgDirectFunc<&pg_relation_filepath>, true, false, TArgs_pg_relation_filepath_Policy>({}); }
struct TArgs_pg_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_relation_size() { return TGenericExec<TPgDirectFunc<&pg_relation_size>, true, true, TArgs_pg_relation_size_Policy>({}); }
struct TArgs_pg_sequence_last_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sequence_last_value() { return TGenericExec<TPgDirectFunc<&pg_sequence_last_value>, true, true, TArgs_pg_sequence_last_value_Policy>({}); }
struct TArgs_pg_sequence_parameters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_sequence_parameters() { return TGenericExec<TPgDirectFunc<&pg_sequence_parameters>, true, false, TArgs_pg_sequence_parameters_Policy>({}); }
struct TArgs_pg_size_bytes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_size_bytes() { return TGenericExec<TPgDirectFunc<&pg_size_bytes>, true, true, TArgs_pg_size_bytes_Policy>({}); }
struct TArgs_pg_size_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_size_pretty() { return TGenericExec<TPgDirectFunc<&pg_size_pretty>, true, false, TArgs_pg_size_pretty_Policy>({}); }
struct TArgs_pg_size_pretty_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_size_pretty_numeric() { return TGenericExec<TPgDirectFunc<&pg_size_pretty_numeric>, true, false, TArgs_pg_size_pretty_numeric_Policy>({}); }
struct TArgs_pg_start_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_start_backup() { return TGenericExec<TPgDirectFunc<&pg_start_backup>, true, true, TArgs_pg_start_backup_Policy>({}); }
struct TArgs_pg_stat_clear_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_clear_snapshot() { return TGenericExec<TPgDirectFunc<&pg_stat_clear_snapshot>, false, true, TArgs_pg_stat_clear_snapshot_Policy>({}); }
struct TArgs_pg_stat_get_analyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_analyze_count() { return TGenericExec<TPgDirectFunc<&pg_stat_get_analyze_count>, true, true, TArgs_pg_stat_get_analyze_count_Policy>({}); }
struct TArgs_pg_stat_get_archiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_archiver() { return TGenericExec<TPgDirectFunc<&pg_stat_get_archiver>, false, false, TArgs_pg_stat_get_archiver_Policy>({}); }
struct TArgs_pg_stat_get_autoanalyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_autoanalyze_count() { return TGenericExec<TPgDirectFunc<&pg_stat_get_autoanalyze_count>, true, true, TArgs_pg_stat_get_autoanalyze_count_Policy>({}); }
struct TArgs_pg_stat_get_autovacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_autovacuum_count() { return TGenericExec<TPgDirectFunc<&pg_stat_get_autovacuum_count>, true, true, TArgs_pg_stat_get_autovacuum_count_Policy>({}); }
struct TArgs_pg_stat_get_backend_activity_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_activity() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_activity>, true, false, TArgs_pg_stat_get_backend_activity_Policy>({}); }
struct TArgs_pg_stat_get_backend_activity_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_activity_start() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_activity_start>, true, true, TArgs_pg_stat_get_backend_activity_start_Policy>({}); }
struct TArgs_pg_stat_get_backend_client_addr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_client_addr() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_client_addr>, true, false, TArgs_pg_stat_get_backend_client_addr_Policy>({}); }
struct TArgs_pg_stat_get_backend_client_port_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_client_port() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_client_port>, true, true, TArgs_pg_stat_get_backend_client_port_Policy>({}); }
struct TArgs_pg_stat_get_backend_dbid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_dbid() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_dbid>, true, true, TArgs_pg_stat_get_backend_dbid_Policy>({}); }
struct TArgs_pg_stat_get_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_pid() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_pid>, true, true, TArgs_pg_stat_get_backend_pid_Policy>({}); }
struct TArgs_pg_stat_get_backend_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_start() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_start>, true, true, TArgs_pg_stat_get_backend_start_Policy>({}); }
struct TArgs_pg_stat_get_backend_userid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_userid() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_userid>, true, true, TArgs_pg_stat_get_backend_userid_Policy>({}); }
struct TArgs_pg_stat_get_backend_wait_event_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_wait_event() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_wait_event>, true, false, TArgs_pg_stat_get_backend_wait_event_Policy>({}); }
struct TArgs_pg_stat_get_backend_wait_event_type_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_wait_event_type() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_wait_event_type>, true, false, TArgs_pg_stat_get_backend_wait_event_type_Policy>({}); }
struct TArgs_pg_stat_get_backend_xact_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_backend_xact_start() { return TGenericExec<TPgDirectFunc<&pg_stat_get_backend_xact_start>, true, true, TArgs_pg_stat_get_backend_xact_start_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_buf_written_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_buf_written_checkpoints() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_buf_written_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_buf_written_checkpoints_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_buf_written_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_buf_written_clean() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_buf_written_clean>, true, true, TArgs_pg_stat_get_bgwriter_buf_written_clean_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_maxwritten_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_maxwritten_clean() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_maxwritten_clean>, true, true, TArgs_pg_stat_get_bgwriter_maxwritten_clean_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_requested_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_requested_checkpoints() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_requested_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_requested_checkpoints_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_stat_reset_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_stat_reset_time>, true, true, TArgs_pg_stat_get_bgwriter_stat_reset_time_Policy>({}); }
struct TArgs_pg_stat_get_bgwriter_timed_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_bgwriter_timed_checkpoints() { return TGenericExec<TPgDirectFunc<&pg_stat_get_bgwriter_timed_checkpoints>, true, true, TArgs_pg_stat_get_bgwriter_timed_checkpoints_Policy>({}); }
struct TArgs_pg_stat_get_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_blocks_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_blocks_fetched>, true, true, TArgs_pg_stat_get_blocks_fetched_Policy>({}); }
struct TArgs_pg_stat_get_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_blocks_hit() { return TGenericExec<TPgDirectFunc<&pg_stat_get_blocks_hit>, true, true, TArgs_pg_stat_get_blocks_hit_Policy>({}); }
struct TArgs_pg_stat_get_buf_alloc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_alloc() { return TGenericExec<TPgDirectFunc<&pg_stat_get_buf_alloc>, true, true, TArgs_pg_stat_get_buf_alloc_Policy>({}); }
struct TArgs_pg_stat_get_buf_fsync_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_fsync_backend() { return TGenericExec<TPgDirectFunc<&pg_stat_get_buf_fsync_backend>, true, true, TArgs_pg_stat_get_buf_fsync_backend_Policy>({}); }
struct TArgs_pg_stat_get_buf_written_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_buf_written_backend() { return TGenericExec<TPgDirectFunc<&pg_stat_get_buf_written_backend>, true, true, TArgs_pg_stat_get_buf_written_backend_Policy>({}); }
struct TArgs_pg_stat_get_checkpoint_sync_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_checkpoint_sync_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_checkpoint_sync_time>, true, true, TArgs_pg_stat_get_checkpoint_sync_time_Policy>({}); }
struct TArgs_pg_stat_get_checkpoint_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_checkpoint_write_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_checkpoint_write_time>, true, true, TArgs_pg_stat_get_checkpoint_write_time_Policy>({}); }
struct TArgs_pg_stat_get_db_active_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_active_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_active_time>, true, true, TArgs_pg_stat_get_db_active_time_Policy>({}); }
struct TArgs_pg_stat_get_db_blk_read_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blk_read_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_blk_read_time>, true, true, TArgs_pg_stat_get_db_blk_read_time_Policy>({}); }
struct TArgs_pg_stat_get_db_blk_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blk_write_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_blk_write_time>, true, true, TArgs_pg_stat_get_db_blk_write_time_Policy>({}); }
struct TArgs_pg_stat_get_db_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blocks_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_blocks_fetched>, true, true, TArgs_pg_stat_get_db_blocks_fetched_Policy>({}); }
struct TArgs_pg_stat_get_db_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_blocks_hit() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_blocks_hit>, true, true, TArgs_pg_stat_get_db_blocks_hit_Policy>({}); }
struct TArgs_pg_stat_get_db_checksum_failures_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_checksum_failures() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_checksum_failures>, true, true, TArgs_pg_stat_get_db_checksum_failures_Policy>({}); }
struct TArgs_pg_stat_get_db_checksum_last_failure_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_checksum_last_failure() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_checksum_last_failure>, true, true, TArgs_pg_stat_get_db_checksum_last_failure_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_all() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_all>, true, true, TArgs_pg_stat_get_db_conflict_all_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_bufferpin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_bufferpin() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_bufferpin>, true, true, TArgs_pg_stat_get_db_conflict_bufferpin_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_lock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_lock() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_lock>, true, true, TArgs_pg_stat_get_db_conflict_lock_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_snapshot() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_snapshot>, true, true, TArgs_pg_stat_get_db_conflict_snapshot_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_startup_deadlock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_startup_deadlock() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_startup_deadlock>, true, true, TArgs_pg_stat_get_db_conflict_startup_deadlock_Policy>({}); }
struct TArgs_pg_stat_get_db_conflict_tablespace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_conflict_tablespace() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_conflict_tablespace>, true, true, TArgs_pg_stat_get_db_conflict_tablespace_Policy>({}); }
struct TArgs_pg_stat_get_db_deadlocks_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_deadlocks() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_deadlocks>, true, true, TArgs_pg_stat_get_db_deadlocks_Policy>({}); }
struct TArgs_pg_stat_get_db_idle_in_transaction_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_idle_in_transaction_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_idle_in_transaction_time>, true, true, TArgs_pg_stat_get_db_idle_in_transaction_time_Policy>({}); }
struct TArgs_pg_stat_get_db_numbackends_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_numbackends() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_numbackends>, true, true, TArgs_pg_stat_get_db_numbackends_Policy>({}); }
struct TArgs_pg_stat_get_db_session_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_session_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_session_time>, true, true, TArgs_pg_stat_get_db_session_time_Policy>({}); }
struct TArgs_pg_stat_get_db_sessions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_sessions>, true, true, TArgs_pg_stat_get_db_sessions_Policy>({}); }
struct TArgs_pg_stat_get_db_sessions_abandoned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_abandoned() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_abandoned>, true, true, TArgs_pg_stat_get_db_sessions_abandoned_Policy>({}); }
struct TArgs_pg_stat_get_db_sessions_fatal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_fatal() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_fatal>, true, true, TArgs_pg_stat_get_db_sessions_fatal_Policy>({}); }
struct TArgs_pg_stat_get_db_sessions_killed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_sessions_killed() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_sessions_killed>, true, true, TArgs_pg_stat_get_db_sessions_killed_Policy>({}); }
struct TArgs_pg_stat_get_db_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_stat_reset_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_stat_reset_time>, true, true, TArgs_pg_stat_get_db_stat_reset_time_Policy>({}); }
struct TArgs_pg_stat_get_db_temp_bytes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_temp_bytes() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_temp_bytes>, true, true, TArgs_pg_stat_get_db_temp_bytes_Policy>({}); }
struct TArgs_pg_stat_get_db_temp_files_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_temp_files() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_temp_files>, true, true, TArgs_pg_stat_get_db_temp_files_Policy>({}); }
struct TArgs_pg_stat_get_db_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_deleted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_deleted>, true, true, TArgs_pg_stat_get_db_tuples_deleted_Policy>({}); }
struct TArgs_pg_stat_get_db_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_fetched>, true, true, TArgs_pg_stat_get_db_tuples_fetched_Policy>({}); }
struct TArgs_pg_stat_get_db_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_inserted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_inserted>, true, true, TArgs_pg_stat_get_db_tuples_inserted_Policy>({}); }
struct TArgs_pg_stat_get_db_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_returned() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_returned>, true, true, TArgs_pg_stat_get_db_tuples_returned_Policy>({}); }
struct TArgs_pg_stat_get_db_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_tuples_updated() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_tuples_updated>, true, true, TArgs_pg_stat_get_db_tuples_updated_Policy>({}); }
struct TArgs_pg_stat_get_db_xact_commit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_xact_commit() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_xact_commit>, true, true, TArgs_pg_stat_get_db_xact_commit_Policy>({}); }
struct TArgs_pg_stat_get_db_xact_rollback_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_db_xact_rollback() { return TGenericExec<TPgDirectFunc<&pg_stat_get_db_xact_rollback>, true, true, TArgs_pg_stat_get_db_xact_rollback_Policy>({}); }
struct TArgs_pg_stat_get_dead_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_dead_tuples() { return TGenericExec<TPgDirectFunc<&pg_stat_get_dead_tuples>, true, true, TArgs_pg_stat_get_dead_tuples_Policy>({}); }
struct TArgs_pg_stat_get_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_calls() { return TGenericExec<TPgDirectFunc<&pg_stat_get_function_calls>, true, true, TArgs_pg_stat_get_function_calls_Policy>({}); }
struct TArgs_pg_stat_get_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_self_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_function_self_time>, true, true, TArgs_pg_stat_get_function_self_time_Policy>({}); }
struct TArgs_pg_stat_get_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_function_total_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_function_total_time>, true, true, TArgs_pg_stat_get_function_total_time_Policy>({}); }
struct TArgs_pg_stat_get_ins_since_vacuum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_ins_since_vacuum() { return TGenericExec<TPgDirectFunc<&pg_stat_get_ins_since_vacuum>, true, true, TArgs_pg_stat_get_ins_since_vacuum_Policy>({}); }
struct TArgs_pg_stat_get_last_analyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_analyze_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_last_analyze_time>, true, true, TArgs_pg_stat_get_last_analyze_time_Policy>({}); }
struct TArgs_pg_stat_get_last_autoanalyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_autoanalyze_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_last_autoanalyze_time>, true, true, TArgs_pg_stat_get_last_autoanalyze_time_Policy>({}); }
struct TArgs_pg_stat_get_last_autovacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_autovacuum_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_last_autovacuum_time>, true, true, TArgs_pg_stat_get_last_autovacuum_time_Policy>({}); }
struct TArgs_pg_stat_get_last_vacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_last_vacuum_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_last_vacuum_time>, true, true, TArgs_pg_stat_get_last_vacuum_time_Policy>({}); }
struct TArgs_pg_stat_get_live_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_live_tuples() { return TGenericExec<TPgDirectFunc<&pg_stat_get_live_tuples>, true, true, TArgs_pg_stat_get_live_tuples_Policy>({}); }
struct TArgs_pg_stat_get_mod_since_analyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_mod_since_analyze() { return TGenericExec<TPgDirectFunc<&pg_stat_get_mod_since_analyze>, true, true, TArgs_pg_stat_get_mod_since_analyze_Policy>({}); }
struct TArgs_pg_stat_get_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_numscans() { return TGenericExec<TPgDirectFunc<&pg_stat_get_numscans>, true, true, TArgs_pg_stat_get_numscans_Policy>({}); }
struct TArgs_pg_stat_get_snapshot_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_snapshot_timestamp() { return TGenericExec<TPgDirectFunc<&pg_stat_get_snapshot_timestamp>, true, true, TArgs_pg_stat_get_snapshot_timestamp_Policy>({}); }
struct TArgs_pg_stat_get_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_deleted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_deleted>, true, true, TArgs_pg_stat_get_tuples_deleted_Policy>({}); }
struct TArgs_pg_stat_get_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_fetched>, true, true, TArgs_pg_stat_get_tuples_fetched_Policy>({}); }
struct TArgs_pg_stat_get_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_hot_updated() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_hot_updated>, true, true, TArgs_pg_stat_get_tuples_hot_updated_Policy>({}); }
struct TArgs_pg_stat_get_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_inserted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_inserted>, true, true, TArgs_pg_stat_get_tuples_inserted_Policy>({}); }
struct TArgs_pg_stat_get_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_returned() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_returned>, true, true, TArgs_pg_stat_get_tuples_returned_Policy>({}); }
struct TArgs_pg_stat_get_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_tuples_updated() { return TGenericExec<TPgDirectFunc<&pg_stat_get_tuples_updated>, true, true, TArgs_pg_stat_get_tuples_updated_Policy>({}); }
struct TArgs_pg_stat_get_vacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_vacuum_count() { return TGenericExec<TPgDirectFunc<&pg_stat_get_vacuum_count>, true, true, TArgs_pg_stat_get_vacuum_count_Policy>({}); }
struct TArgs_pg_stat_get_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_wal() { return TGenericExec<TPgDirectFunc<&pg_stat_get_wal>, false, false, TArgs_pg_stat_get_wal_Policy>({}); }
struct TArgs_pg_stat_get_xact_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_blocks_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_blocks_fetched>, true, true, TArgs_pg_stat_get_xact_blocks_fetched_Policy>({}); }
struct TArgs_pg_stat_get_xact_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_blocks_hit() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_blocks_hit>, true, true, TArgs_pg_stat_get_xact_blocks_hit_Policy>({}); }
struct TArgs_pg_stat_get_xact_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_calls() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_function_calls>, true, true, TArgs_pg_stat_get_xact_function_calls_Policy>({}); }
struct TArgs_pg_stat_get_xact_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_self_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_function_self_time>, true, true, TArgs_pg_stat_get_xact_function_self_time_Policy>({}); }
struct TArgs_pg_stat_get_xact_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_function_total_time() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_function_total_time>, true, true, TArgs_pg_stat_get_xact_function_total_time_Policy>({}); }
struct TArgs_pg_stat_get_xact_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_numscans() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_numscans>, true, true, TArgs_pg_stat_get_xact_numscans_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_deleted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_deleted>, true, true, TArgs_pg_stat_get_xact_tuples_deleted_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_fetched() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_fetched>, true, true, TArgs_pg_stat_get_xact_tuples_fetched_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_hot_updated() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_hot_updated>, true, true, TArgs_pg_stat_get_xact_tuples_hot_updated_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_inserted() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_inserted>, true, true, TArgs_pg_stat_get_xact_tuples_inserted_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_returned() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_returned>, true, true, TArgs_pg_stat_get_xact_tuples_returned_Policy>({}); }
struct TArgs_pg_stat_get_xact_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_get_xact_tuples_updated() { return TGenericExec<TPgDirectFunc<&pg_stat_get_xact_tuples_updated>, true, true, TArgs_pg_stat_get_xact_tuples_updated_Policy>({}); }
struct TArgs_pg_stat_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_reset() { return TGenericExec<TPgDirectFunc<&pg_stat_reset>, false, true, TArgs_pg_stat_reset_Policy>({}); }
struct TArgs_pg_stat_reset_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_replication_slot() { return TGenericExec<TPgDirectFunc<&pg_stat_reset_replication_slot>, false, true, TArgs_pg_stat_reset_replication_slot_Policy>({}); }
struct TArgs_pg_stat_reset_shared_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_shared() { return TGenericExec<TPgDirectFunc<&pg_stat_reset_shared>, true, true, TArgs_pg_stat_reset_shared_Policy>({}); }
struct TArgs_pg_stat_reset_single_function_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_reset_single_function_counters() { return TGenericExec<TPgDirectFunc<&pg_stat_reset_single_function_counters>, true, true, TArgs_pg_stat_reset_single_function_counters_Policy>({}); }
struct TArgs_pg_stat_reset_single_table_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_stat_reset_single_table_counters() { return TGenericExec<TPgDirectFunc<&pg_stat_reset_single_table_counters>, true, true, TArgs_pg_stat_reset_single_table_counters_Policy>({}); }
struct TArgs_pg_stat_reset_slru_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_stat_reset_slru() { return TGenericExec<TPgDirectFunc<&pg_stat_reset_slru>, false, true, TArgs_pg_stat_reset_slru_Policy>({}); }
struct TArgs_pg_statistics_obj_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_statistics_obj_is_visible() { return TGenericExec<TPgDirectFunc<&pg_statistics_obj_is_visible>, true, true, TArgs_pg_statistics_obj_is_visible_Policy>({}); }
struct TArgs_pg_stop_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stop_backup() { return TGenericExec<TPgDirectFunc<&pg_stop_backup>, true, true, TArgs_pg_stop_backup_Policy>({}); }
struct TArgs_pg_switch_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_switch_wal() { return TGenericExec<TPgDirectFunc<&pg_switch_wal>, true, true, TArgs_pg_switch_wal_Policy>({}); }
struct TArgs_pg_table_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_is_visible() { return TGenericExec<TPgDirectFunc<&pg_table_is_visible>, true, true, TArgs_pg_table_is_visible_Policy>({}); }
struct TArgs_pg_table_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_size() { return TGenericExec<TPgDirectFunc<&pg_table_size>, true, true, TArgs_pg_table_size_Policy>({}); }
struct TArgs_pg_tablespace_size_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_tablespace_size_name() { return TGenericExec<TPgDirectFunc<&pg_tablespace_size_name>, true, true, TArgs_pg_tablespace_size_name_Policy>({}); }
struct TArgs_pg_tablespace_size_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_tablespace_size_oid() { return TGenericExec<TPgDirectFunc<&pg_tablespace_size_oid>, true, true, TArgs_pg_tablespace_size_oid_Policy>({}); }
struct TArgs_pg_total_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_total_relation_size() { return TGenericExec<TPgDirectFunc<&pg_total_relation_size>, true, true, TArgs_pg_total_relation_size_Policy>({}); }
struct TArgs_pg_trigger_depth_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_trigger_depth() { return TGenericExec<TPgDirectFunc<&pg_trigger_depth>, true, true, TArgs_pg_trigger_depth_Policy>({}); }
struct TArgs_pg_ts_config_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_config_is_visible() { return TGenericExec<TPgDirectFunc<&pg_ts_config_is_visible>, true, true, TArgs_pg_ts_config_is_visible_Policy>({}); }
struct TArgs_pg_ts_dict_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_dict_is_visible() { return TGenericExec<TPgDirectFunc<&pg_ts_dict_is_visible>, true, true, TArgs_pg_ts_dict_is_visible_Policy>({}); }
struct TArgs_pg_ts_parser_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_parser_is_visible() { return TGenericExec<TPgDirectFunc<&pg_ts_parser_is_visible>, true, true, TArgs_pg_ts_parser_is_visible_Policy>({}); }
struct TArgs_pg_ts_template_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ts_template_is_visible() { return TGenericExec<TPgDirectFunc<&pg_ts_template_is_visible>, true, true, TArgs_pg_ts_template_is_visible_Policy>({}); }
struct TArgs_pg_type_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_type_is_visible() { return TGenericExec<TPgDirectFunc<&pg_type_is_visible>, true, true, TArgs_pg_type_is_visible_Policy>({}); }
struct TArgs_pg_wal_lsn_diff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_wal_lsn_diff() { return TGenericExec<TPgDirectFunc<&pg_wal_lsn_diff>, true, false, TArgs_pg_wal_lsn_diff_Policy>({}); }
struct TArgs_pg_wal_replay_pause_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_wal_replay_pause() { return TGenericExec<TPgDirectFunc<&pg_wal_replay_pause>, true, true, TArgs_pg_wal_replay_pause_Policy>({}); }
struct TArgs_pg_wal_replay_resume_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_wal_replay_resume() { return TGenericExec<TPgDirectFunc<&pg_wal_replay_resume>, true, true, TArgs_pg_wal_replay_resume_Policy>({}); }
struct TArgs_pg_walfile_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_walfile_name() { return TGenericExec<TPgDirectFunc<&pg_walfile_name>, true, false, TArgs_pg_walfile_name_Policy>({}); }
struct TArgs_pg_walfile_name_offset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_walfile_name_offset() { return TGenericExec<TPgDirectFunc<&pg_walfile_name_offset>, true, false, TArgs_pg_walfile_name_offset_Policy>({}); }
struct TArgs_prsd_end_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_prsd_end() { return TGenericExec<TPgDirectFunc<&prsd_end>, true, true, TArgs_prsd_end_Policy>({}); }
struct TArgs_prsd_headline_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_prsd_headline() { return TGenericExec<TPgDirectFunc<&prsd_headline>, true, true, TArgs_prsd_headline_Policy>({}); }
struct TArgs_prsd_lextype_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_prsd_lextype() { return TGenericExec<TPgDirectFunc<&prsd_lextype>, true, true, TArgs_prsd_lextype_Policy>({}); }
struct TArgs_prsd_nexttoken_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_prsd_nexttoken() { return TGenericExec<TPgDirectFunc<&prsd_nexttoken>, true, true, TArgs_prsd_nexttoken_Policy>({}); }
struct TArgs_prsd_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_prsd_start() { return TGenericExec<TPgDirectFunc<&prsd_start>, true, true, TArgs_prsd_start_Policy>({}); }
struct TArgs_radians_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_radians() { return TGenericExec<TPgDirectFunc<&radians>, true, true, TArgs_radians_Policy>({}); }
struct TArgs_range_adjacent_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_adjacent_multirange() { return TGenericExec<TPgDirectFunc<&range_adjacent_multirange>, true, true, TArgs_range_adjacent_multirange_Policy>({}); }
struct TArgs_range_after_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_after_multirange() { return TGenericExec<TPgDirectFunc<&range_after_multirange>, true, true, TArgs_range_after_multirange_Policy>({}); }
struct TArgs_range_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_range_agg_finalfn() { return TGenericExec<TPgDirectFunc<&range_agg_finalfn>, false, false, TArgs_range_agg_finalfn_Policy>({}); }
struct TArgs_range_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_range_agg_transfn() { return TGenericExec<TPgDirectFunc<&range_agg_transfn>, false, true, TArgs_range_agg_transfn_Policy>({}); }
struct TArgs_range_before_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_before_multirange() { return TGenericExec<TPgDirectFunc<&range_before_multirange>, true, true, TArgs_range_before_multirange_Policy>({}); }
struct TArgs_range_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contained_by_multirange() { return TGenericExec<TPgDirectFunc<&range_contained_by_multirange>, true, true, TArgs_range_contained_by_multirange_Policy>({}); }
struct TArgs_range_contains_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_contains_multirange() { return TGenericExec<TPgDirectFunc<&range_contains_multirange>, true, true, TArgs_range_contains_multirange_Policy>({}); }
struct TArgs_range_merge_from_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_range_merge_from_multirange() { return TGenericExec<TPgDirectFunc<&range_merge_from_multirange>, true, false, TArgs_range_merge_from_multirange_Policy>({}); }
struct TArgs_range_overlaps_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overlaps_multirange() { return TGenericExec<TPgDirectFunc<&range_overlaps_multirange>, true, true, TArgs_range_overlaps_multirange_Policy>({}); }
struct TArgs_range_overleft_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overleft_multirange() { return TGenericExec<TPgDirectFunc<&range_overleft_multirange>, true, true, TArgs_range_overleft_multirange_Policy>({}); }
struct TArgs_range_overright_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_range_overright_multirange() { return TGenericExec<TPgDirectFunc<&range_overright_multirange>, true, true, TArgs_range_overright_multirange_Policy>({}); }
struct TArgs_rangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_rangesel() { return TGenericExec<TPgDirectFunc<&rangesel>, true, true, TArgs_rangesel_Policy>({}); }
struct TArgs_record_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_eq() { return TGenericExec<TPgDirectFunc<&record_eq>, true, true, TArgs_record_eq_Policy>({}); }
struct TArgs_record_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_ge() { return TGenericExec<TPgDirectFunc<&record_ge>, true, true, TArgs_record_ge_Policy>({}); }
struct TArgs_record_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_gt() { return TGenericExec<TPgDirectFunc<&record_gt>, true, true, TArgs_record_gt_Policy>({}); }
struct TArgs_record_image_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_eq() { return TGenericExec<TPgDirectFunc<&record_image_eq>, true, true, TArgs_record_image_eq_Policy>({}); }
struct TArgs_record_image_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_ge() { return TGenericExec<TPgDirectFunc<&record_image_ge>, true, true, TArgs_record_image_ge_Policy>({}); }
struct TArgs_record_image_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_gt() { return TGenericExec<TPgDirectFunc<&record_image_gt>, true, true, TArgs_record_image_gt_Policy>({}); }
struct TArgs_record_image_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_le() { return TGenericExec<TPgDirectFunc<&record_image_le>, true, true, TArgs_record_image_le_Policy>({}); }
struct TArgs_record_image_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_lt() { return TGenericExec<TPgDirectFunc<&record_image_lt>, true, true, TArgs_record_image_lt_Policy>({}); }
struct TArgs_record_image_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_image_ne() { return TGenericExec<TPgDirectFunc<&record_image_ne>, true, true, TArgs_record_image_ne_Policy>({}); }
struct TArgs_record_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_record_in() { return TGenericExec<TPgDirectFunc<&record_in>, true, false, TArgs_record_in_Policy>({}); }
struct TArgs_record_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_le() { return TGenericExec<TPgDirectFunc<&record_le>, true, true, TArgs_record_le_Policy>({}); }
struct TArgs_record_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_lt() { return TGenericExec<TPgDirectFunc<&record_lt>, true, true, TArgs_record_lt_Policy>({}); }
struct TArgs_record_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_record_ne() { return TGenericExec<TPgDirectFunc<&record_ne>, true, true, TArgs_record_ne_Policy>({}); }
struct TArgs_record_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_record_out() { return TGenericExec<TPgDirectFunc<&record_out>, true, false, TArgs_record_out_Policy>({}); }
struct TArgs_record_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_record_recv() { return TGenericExec<TPgDirectFunc<&record_recv>, true, false, TArgs_record_recv_Policy>({}); }
struct TArgs_record_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_record_send() { return TGenericExec<TPgDirectFunc<&record_send>, true, false, TArgs_record_send_Policy>({}); }
struct TArgs_session_user_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_session_user() { return TGenericExec<TPgDirectFunc<&session_user>, true, false, TArgs_session_user_Policy>({}); }
struct TArgs_setseed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_setseed() { return TGenericExec<TPgDirectFunc<&setseed>, true, true, TArgs_setseed_Policy>({}); }
struct TArgs_setval3_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_setval3_oid() { return TGenericExec<TPgDirectFunc<&setval3_oid>, true, true, TArgs_setval3_oid_Policy>({}); }
struct TArgs_setval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_setval_oid() { return TGenericExec<TPgDirectFunc<&setval_oid>, true, true, TArgs_setval_oid_Policy>({}); }
struct TArgs_shell_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_shell_in() { return TGenericExec<TPgDirectFunc<&shell_in>, false, true, TArgs_shell_in_Policy>({}); }
struct TArgs_shell_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_shell_out() { return TGenericExec<TPgDirectFunc<&shell_out>, true, false, TArgs_shell_out_Policy>({}); }
struct TArgs_spg_kd_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_choose() { return TGenericExec<TPgDirectFunc<&spg_kd_choose>, true, true, TArgs_spg_kd_choose_Policy>({}); }
struct TArgs_spg_kd_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_config() { return TGenericExec<TPgDirectFunc<&spg_kd_config>, true, true, TArgs_spg_kd_config_Policy>({}); }
struct TArgs_spg_kd_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_inner_consistent() { return TGenericExec<TPgDirectFunc<&spg_kd_inner_consistent>, true, true, TArgs_spg_kd_inner_consistent_Policy>({}); }
struct TArgs_spg_kd_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_kd_picksplit() { return TGenericExec<TPgDirectFunc<&spg_kd_picksplit>, true, true, TArgs_spg_kd_picksplit_Policy>({}); }
struct TArgs_spg_quad_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_choose() { return TGenericExec<TPgDirectFunc<&spg_quad_choose>, true, true, TArgs_spg_quad_choose_Policy>({}); }
struct TArgs_spg_quad_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_config() { return TGenericExec<TPgDirectFunc<&spg_quad_config>, true, true, TArgs_spg_quad_config_Policy>({}); }
struct TArgs_spg_quad_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_inner_consistent() { return TGenericExec<TPgDirectFunc<&spg_quad_inner_consistent>, true, true, TArgs_spg_quad_inner_consistent_Policy>({}); }
struct TArgs_spg_quad_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_leaf_consistent() { return TGenericExec<TPgDirectFunc<&spg_quad_leaf_consistent>, true, true, TArgs_spg_quad_leaf_consistent_Policy>({}); }
struct TArgs_spg_quad_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_quad_picksplit() { return TGenericExec<TPgDirectFunc<&spg_quad_picksplit>, true, true, TArgs_spg_quad_picksplit_Policy>({}); }
struct TArgs_spg_text_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_choose() { return TGenericExec<TPgDirectFunc<&spg_text_choose>, true, true, TArgs_spg_text_choose_Policy>({}); }
struct TArgs_spg_text_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_config() { return TGenericExec<TPgDirectFunc<&spg_text_config>, true, true, TArgs_spg_text_config_Policy>({}); }
struct TArgs_spg_text_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_inner_consistent() { return TGenericExec<TPgDirectFunc<&spg_text_inner_consistent>, true, true, TArgs_spg_text_inner_consistent_Policy>({}); }
struct TArgs_spg_text_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_leaf_consistent() { return TGenericExec<TPgDirectFunc<&spg_text_leaf_consistent>, true, true, TArgs_spg_text_leaf_consistent_Policy>({}); }
struct TArgs_spg_text_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_spg_text_picksplit() { return TGenericExec<TPgDirectFunc<&spg_text_picksplit>, true, true, TArgs_spg_text_picksplit_Policy>({}); }
struct TArgs_spghandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_spghandler() { return TGenericExec<TPgDirectFunc<&spghandler>, true, true, TArgs_spghandler_Policy>({}); }
struct TArgs_suppress_redundant_updates_trigger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_suppress_redundant_updates_trigger() { return TGenericExec<TPgDirectFunc<&suppress_redundant_updates_trigger>, true, true, TArgs_suppress_redundant_updates_trigger_Policy>({}); }
struct TArgs_table_am_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_table_am_handler_in() { return TGenericExec<TPgDirectFunc<&table_am_handler_in>, false, true, TArgs_table_am_handler_in_Policy>({}); }
struct TArgs_table_am_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_table_am_handler_out() { return TGenericExec<TPgDirectFunc<&table_am_handler_out>, true, false, TArgs_table_am_handler_out_Policy>({}); }
struct TArgs_trigger_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_trigger_in() { return TGenericExec<TPgDirectFunc<&trigger_in>, false, true, TArgs_trigger_in_Policy>({}); }
struct TArgs_trigger_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_trigger_out() { return TGenericExec<TPgDirectFunc<&trigger_out>, true, false, TArgs_trigger_out_Policy>({}); }
struct TArgs_tsm_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsm_handler_in() { return TGenericExec<TPgDirectFunc<&tsm_handler_in>, false, true, TArgs_tsm_handler_in_Policy>({}); }
struct TArgs_tsm_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_handler_out() { return TGenericExec<TPgDirectFunc<&tsm_handler_out>, true, false, TArgs_tsm_handler_out_Policy>({}); }
struct TArgs_tsm_system_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_system_handler() { return TGenericExec<TPgDirectFunc<&tsm_system_handler>, true, true, TArgs_tsm_system_handler_Policy>({}); }
struct TArgs_tsq_mcontained_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsq_mcontained() { return TGenericExec<TPgDirectFunc<&tsq_mcontained>, true, true, TArgs_tsq_mcontained_Policy>({}); }
struct TArgs_tsq_mcontains_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsq_mcontains() { return TGenericExec<TPgDirectFunc<&tsq_mcontains>, true, true, TArgs_tsq_mcontains_Policy>({}); }
struct TArgs_tsquery_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_and() { return TGenericExec<TPgDirectFunc<&tsquery_and>, true, false, TArgs_tsquery_and_Policy>({}); }
struct TArgs_tsquery_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_cmp() { return TGenericExec<TPgDirectFunc<&tsquery_cmp>, true, true, TArgs_tsquery_cmp_Policy>({}); }
struct TArgs_tsquery_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_eq() { return TGenericExec<TPgDirectFunc<&tsquery_eq>, true, true, TArgs_tsquery_eq_Policy>({}); }
struct TArgs_tsquery_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_ge() { return TGenericExec<TPgDirectFunc<&tsquery_ge>, true, true, TArgs_tsquery_ge_Policy>({}); }
struct TArgs_tsquery_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_gt() { return TGenericExec<TPgDirectFunc<&tsquery_gt>, true, true, TArgs_tsquery_gt_Policy>({}); }
struct TArgs_tsquery_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_le() { return TGenericExec<TPgDirectFunc<&tsquery_le>, true, true, TArgs_tsquery_le_Policy>({}); }
struct TArgs_tsquery_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_lt() { return TGenericExec<TPgDirectFunc<&tsquery_lt>, true, true, TArgs_tsquery_lt_Policy>({}); }
struct TArgs_tsquery_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_ne() { return TGenericExec<TPgDirectFunc<&tsquery_ne>, true, true, TArgs_tsquery_ne_Policy>({}); }
struct TArgs_tsquery_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquery_not() { return TGenericExec<TPgDirectFunc<&tsquery_not>, true, false, TArgs_tsquery_not_Policy>({}); }
struct TArgs_tsquery_numnode_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquery_numnode() { return TGenericExec<TPgDirectFunc<&tsquery_numnode>, true, true, TArgs_tsquery_numnode_Policy>({}); }
struct TArgs_tsquery_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_or() { return TGenericExec<TPgDirectFunc<&tsquery_or>, true, false, TArgs_tsquery_or_Policy>({}); }
struct TArgs_tsquery_phrase_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_tsquery_phrase() { return TGenericExec<TPgDirectFunc<&tsquery_phrase>, true, false, TArgs_tsquery_phrase_Policy>({}); }
struct TArgs_tsquery_phrase_distance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_tsquery_phrase_distance() { return TGenericExec<TPgDirectFunc<&tsquery_phrase_distance>, true, false, TArgs_tsquery_phrase_distance_Policy>({}); }
struct TArgs_tsvectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorin() { return TGenericExec<TPgDirectFunc<&tsvectorin>, true, false, TArgs_tsvectorin_Policy>({}); }
struct TArgs_tsvectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorout() { return TGenericExec<TPgDirectFunc<&tsvectorout>, true, false, TArgs_tsvectorout_Policy>({}); }
struct TArgs_tsvectorrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsvectorrecv() { return TGenericExec<TPgDirectFunc<&tsvectorrecv>, true, false, TArgs_tsvectorrecv_Policy>({}); }
struct TArgs_tsvectorsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsvectorsend() { return TGenericExec<TPgDirectFunc<&tsvectorsend>, true, false, TArgs_tsvectorsend_Policy>({}); }
struct TArgs_varchar_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varchar() { return TGenericExec<TPgDirectFunc<&varchar>, true, false, TArgs_varchar_Policy>({}); }
struct TArgs_varchar_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varchar_support() { return TGenericExec<TPgDirectFunc<&varchar_support>, true, true, TArgs_varchar_support_Policy>({}); }
struct TArgs_varcharin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varcharin() { return TGenericExec<TPgDirectFunc<&varcharin>, true, false, TArgs_varcharin_Policy>({}); }
struct TArgs_varcharout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varcharout() { return TGenericExec<TPgDirectFunc<&varcharout>, true, false, TArgs_varcharout_Policy>({}); }
struct TArgs_varcharrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_varcharrecv() { return TGenericExec<TPgDirectFunc<&varcharrecv>, true, false, TArgs_varcharrecv_Policy>({}); }
struct TArgs_varcharsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varcharsend() { return TGenericExec<TPgDirectFunc<&varcharsend>, true, false, TArgs_varcharsend_Policy>({}); }
struct TArgs_varchartypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varchartypmodin() { return TGenericExec<TPgDirectFunc<&varchartypmodin>, true, true, TArgs_varchartypmodin_Policy>({}); }
struct TArgs_varchartypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varchartypmodout() { return TGenericExec<TPgDirectFunc<&varchartypmodout>, true, false, TArgs_varchartypmodout_Policy>({}); }
struct TArgs_void_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_void_in() { return TGenericExec<TPgDirectFunc<&void_in>, true, true, TArgs_void_in_Policy>({}); }
struct TArgs_void_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_out() { return TGenericExec<TPgDirectFunc<&void_out>, true, false, TArgs_void_out_Policy>({}); }
struct TArgs_void_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_recv() { return TGenericExec<TPgDirectFunc<&void_recv>, true, true, TArgs_void_recv_Policy>({}); }
struct TArgs_void_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_send() { return TGenericExec<TPgDirectFunc<&void_send>, true, false, TArgs_void_send_Policy>({}); }
struct TArgs_width_bucket_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_width_bucket_float8() { return TGenericExec<TPgDirectFunc<&width_bucket_float8>, true, true, TArgs_width_bucket_float8_Policy>({}); }
struct TArgs_xid8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8cmp() { return TGenericExec<TPgDirectFunc<&xid8cmp>, true, true, TArgs_xid8cmp_Policy>({}); }
struct TArgs_xid8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8eq() { return TGenericExec<TPgDirectFunc<&xid8eq>, true, true, TArgs_xid8eq_Policy>({}); }
struct TArgs_xid8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8ge() { return TGenericExec<TPgDirectFunc<&xid8ge>, true, true, TArgs_xid8ge_Policy>({}); }
struct TArgs_xid8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8gt() { return TGenericExec<TPgDirectFunc<&xid8gt>, true, true, TArgs_xid8gt_Policy>({}); }
struct TArgs_xid8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xid8in() { return TGenericExec<TPgDirectFunc<&xid8in>, true, true, TArgs_xid8in_Policy>({}); }
struct TArgs_xid8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8le() { return TGenericExec<TPgDirectFunc<&xid8le>, true, true, TArgs_xid8le_Policy>({}); }
struct TArgs_xid8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8lt() { return TGenericExec<TPgDirectFunc<&xid8lt>, true, true, TArgs_xid8lt_Policy>({}); }
struct TArgs_xid8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xid8ne() { return TGenericExec<TPgDirectFunc<&xid8ne>, true, true, TArgs_xid8ne_Policy>({}); }
struct TArgs_xid8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8out() { return TGenericExec<TPgDirectFunc<&xid8out>, true, false, TArgs_xid8out_Policy>({}); }
struct TArgs_xid8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8recv() { return TGenericExec<TPgDirectFunc<&xid8recv>, true, true, TArgs_xid8recv_Policy>({}); }
struct TArgs_xid8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8send() { return TGenericExec<TPgDirectFunc<&xid8send>, true, false, TArgs_xid8send_Policy>({}); }
struct TArgs_xid8toxid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid8toxid() { return TGenericExec<TPgDirectFunc<&xid8toxid>, true, true, TArgs_xid8toxid_Policy>({}); }
struct TArgs_xid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xid_age() { return TGenericExec<TPgDirectFunc<&xid_age>, true, true, TArgs_xid_age_Policy>({}); }
struct TArgs_xideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xideq() { return TGenericExec<TPgDirectFunc<&xideq>, true, true, TArgs_xideq_Policy>({}); }
struct TArgs_xidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_xidin() { return TGenericExec<TPgDirectFunc<&xidin>, true, true, TArgs_xidin_Policy>({}); }
struct TArgs_xidneq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_xidneq() { return TGenericExec<TPgDirectFunc<&xidneq>, true, true, TArgs_xidneq_Policy>({}); }
struct TArgs_xidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidout() { return TGenericExec<TPgDirectFunc<&xidout>, true, false, TArgs_xidout_Policy>({}); }
struct TArgs_xidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidrecv() { return TGenericExec<TPgDirectFunc<&xidrecv>, true, true, TArgs_xidrecv_Policy>({}); }
struct TArgs_xidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_xidsend() { return TGenericExec<TPgDirectFunc<&xidsend>, true, false, TArgs_xidsend_Policy>({}); }
