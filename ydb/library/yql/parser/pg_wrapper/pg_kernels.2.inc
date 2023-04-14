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
struct TArgs_btboolcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_btboolcmp() { return &GenericExec<TPgDirectFunc<&btboolcmp>, true, true, TArgs_btboolcmp_Policy>; }
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
struct TArgs_bytealike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealike() { return &GenericExec<TPgDirectFunc<&bytealike>, true, true, TArgs_bytealike_Policy>; }
struct TArgs_byteanlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteanlike() { return &GenericExec<TPgDirectFunc<&byteanlike>, true, true, TArgs_byteanlike_Policy>; }
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
struct TArgs_clock_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_clock_timestamp() { return &GenericExec<TPgDirectFunc<&clock_timestamp>, true, true, TArgs_clock_timestamp_Policy>; }
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
struct TArgs_dtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dtoi8() { return &GenericExec<TPgDirectFunc<&dtoi8>, true, true, TArgs_dtoi8_Policy>; }
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
struct TArgs_extract_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_extract_interval() { return &GenericExec<TPgDirectFunc<&extract_interval>, true, false, TArgs_extract_interval_Policy>; }
struct TArgs_extract_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_timestamp() { return &GenericExec<TPgDirectFunc<&extract_timestamp>, true, false, TArgs_extract_timestamp_Policy>; }
struct TArgs_extract_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_extract_timestamptz() { return &GenericExec<TPgDirectFunc<&extract_timestamptz>, true, false, TArgs_extract_timestamptz_Policy>; }
struct TArgs_float8_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8_timestamptz() { return &GenericExec<TPgDirectFunc<&float8_timestamptz>, true, true, TArgs_float8_timestamptz_Policy>; }
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
struct TArgs_ftoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_ftoi8() { return &GenericExec<TPgDirectFunc<&ftoi8>, true, true, TArgs_ftoi8_Policy>; }
struct TArgs_gen_random_uuid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_gen_random_uuid() { return &GenericExec<TPgDirectFunc<&gen_random_uuid>, true, false, TArgs_gen_random_uuid_Policy>; }
struct TArgs_generate_series_int8_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int8_support() { return &GenericExec<TPgDirectFunc<&generate_series_int8_support>, true, true, TArgs_generate_series_int8_support_Policy>; }
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
struct TArgs_heap_tableam_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_heap_tableam_handler() { return &GenericExec<TPgDirectFunc<&heap_tableam_handler>, true, true, TArgs_heap_tableam_handler_Policy>; }
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
struct TArgs_iclikejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_iclikejoinsel() { return &GenericExec<TPgDirectFunc<&iclikejoinsel>, true, true, TArgs_iclikejoinsel_Policy>; }
struct TArgs_iclikesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_iclikesel() { return &GenericExec<TPgDirectFunc<&iclikesel>, true, true, TArgs_iclikesel_Policy>; }
struct TArgs_icnlikejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_icnlikejoinsel() { return &GenericExec<TPgDirectFunc<&icnlikejoinsel>, true, true, TArgs_icnlikejoinsel_Policy>; }
struct TArgs_icnlikesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_icnlikesel() { return &GenericExec<TPgDirectFunc<&icnlikesel>, true, true, TArgs_icnlikesel_Policy>; }
struct TArgs_icregexeqjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_icregexeqjoinsel() { return &GenericExec<TPgDirectFunc<&icregexeqjoinsel>, true, true, TArgs_icregexeqjoinsel_Policy>; }
struct TArgs_icregexeqsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_icregexeqsel() { return &GenericExec<TPgDirectFunc<&icregexeqsel>, true, true, TArgs_icregexeqsel_Policy>; }
struct TArgs_icregexnejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_icregexnejoinsel() { return &GenericExec<TPgDirectFunc<&icregexnejoinsel>, true, true, TArgs_icregexnejoinsel_Policy>; }
struct TArgs_icregexnesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_icregexnesel() { return &GenericExec<TPgDirectFunc<&icregexnesel>, true, true, TArgs_icregexnesel_Policy>; }
struct TArgs_in_range_int8_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_in_range_int8_int8() { return &GenericExec<TPgDirectFunc<&in_range_int8_int8>, true, true, TArgs_in_range_int8_int8_Policy>; }
struct TArgs_in_range_interval_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_interval_interval() { return &GenericExec<TPgDirectFunc<&in_range_interval_interval>, true, true, TArgs_in_range_interval_interval_Policy>; }
struct TArgs_in_range_timestamp_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_timestamp_interval() { return &GenericExec<TPgDirectFunc<&in_range_timestamp_interval>, true, true, TArgs_in_range_timestamp_interval_Policy>; }
struct TArgs_in_range_timestamptz_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,false,true,true};
};
TExecFunc arrow_in_range_timestamptz_interval() { return &GenericExec<TPgDirectFunc<&in_range_timestamptz_interval>, true, true, TArgs_in_range_timestamptz_interval_Policy>; }
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
struct TArgs_int2_mul_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2_mul_cash() { return &GenericExec<TPgDirectFunc<&int2_mul_cash>, true, true, TArgs_int2_mul_cash_Policy>; }
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
struct TArgs_interval_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_accum() { return &GenericExec<TPgDirectFunc<&interval_accum>, true, false, TArgs_interval_accum_Policy>; }
struct TArgs_interval_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_accum_inv() { return &GenericExec<TPgDirectFunc<&interval_accum_inv>, true, false, TArgs_interval_accum_inv_Policy>; }
struct TArgs_interval_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_avg() { return &GenericExec<TPgDirectFunc<&interval_avg>, true, false, TArgs_interval_avg_Policy>; }
struct TArgs_interval_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_cmp() { return &GenericExec<TPgDirectFunc<&interval_cmp>, true, true, TArgs_interval_cmp_Policy>; }
struct TArgs_interval_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_combine() { return &GenericExec<TPgDirectFunc<&interval_combine>, true, false, TArgs_interval_combine_Policy>; }
struct TArgs_interval_div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_interval_div() { return &GenericExec<TPgDirectFunc<&interval_div>, true, false, TArgs_interval_div_Policy>; }
struct TArgs_interval_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_eq() { return &GenericExec<TPgDirectFunc<&interval_eq>, true, true, TArgs_interval_eq_Policy>; }
struct TArgs_interval_finite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_finite() { return &GenericExec<TPgDirectFunc<&interval_finite>, true, true, TArgs_interval_finite_Policy>; }
struct TArgs_interval_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_ge() { return &GenericExec<TPgDirectFunc<&interval_ge>, true, true, TArgs_interval_ge_Policy>; }
struct TArgs_interval_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_gt() { return &GenericExec<TPgDirectFunc<&interval_gt>, true, true, TArgs_interval_gt_Policy>; }
struct TArgs_interval_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_hash() { return &GenericExec<TPgDirectFunc<&interval_hash>, true, true, TArgs_interval_hash_Policy>; }
struct TArgs_interval_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_interval_hash_extended() { return &GenericExec<TPgDirectFunc<&interval_hash_extended>, true, true, TArgs_interval_hash_extended_Policy>; }
struct TArgs_interval_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_interval_in() { return &GenericExec<TPgDirectFunc<&interval_in>, true, false, TArgs_interval_in_Policy>; }
struct TArgs_interval_justify_days_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_justify_days() { return &GenericExec<TPgDirectFunc<&interval_justify_days>, true, false, TArgs_interval_justify_days_Policy>; }
struct TArgs_interval_justify_hours_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_justify_hours() { return &GenericExec<TPgDirectFunc<&interval_justify_hours>, true, false, TArgs_interval_justify_hours_Policy>; }
struct TArgs_interval_justify_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_justify_interval() { return &GenericExec<TPgDirectFunc<&interval_justify_interval>, true, false, TArgs_interval_justify_interval_Policy>; }
struct TArgs_interval_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_larger() { return &GenericExec<TPgDirectFunc<&interval_larger>, true, false, TArgs_interval_larger_Policy>; }
struct TArgs_interval_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_le() { return &GenericExec<TPgDirectFunc<&interval_le>, true, true, TArgs_interval_le_Policy>; }
struct TArgs_interval_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_lt() { return &GenericExec<TPgDirectFunc<&interval_lt>, true, true, TArgs_interval_lt_Policy>; }
struct TArgs_interval_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_mi() { return &GenericExec<TPgDirectFunc<&interval_mi>, true, false, TArgs_interval_mi_Policy>; }
struct TArgs_interval_mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_interval_mul() { return &GenericExec<TPgDirectFunc<&interval_mul>, true, false, TArgs_interval_mul_Policy>; }
struct TArgs_interval_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_ne() { return &GenericExec<TPgDirectFunc<&interval_ne>, true, true, TArgs_interval_ne_Policy>; }
struct TArgs_interval_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_out() { return &GenericExec<TPgDirectFunc<&interval_out>, true, false, TArgs_interval_out_Policy>; }
struct TArgs_interval_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_part() { return &GenericExec<TPgDirectFunc<&interval_part>, true, true, TArgs_interval_part_Policy>; }
struct TArgs_interval_pl_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_pl() { return &GenericExec<TPgDirectFunc<&interval_pl>, true, false, TArgs_interval_pl_Policy>; }
struct TArgs_interval_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_interval_recv() { return &GenericExec<TPgDirectFunc<&interval_recv>, true, false, TArgs_interval_recv_Policy>; }
struct TArgs_interval_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_interval_scale() { return &GenericExec<TPgDirectFunc<&interval_scale>, true, false, TArgs_interval_scale_Policy>; }
struct TArgs_interval_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_send() { return &GenericExec<TPgDirectFunc<&interval_send>, true, false, TArgs_interval_send_Policy>; }
struct TArgs_interval_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_smaller() { return &GenericExec<TPgDirectFunc<&interval_smaller>, true, false, TArgs_interval_smaller_Policy>; }
struct TArgs_interval_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_interval_support() { return &GenericExec<TPgDirectFunc<&interval_support>, true, true, TArgs_interval_support_Policy>; }
struct TArgs_interval_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_trunc() { return &GenericExec<TPgDirectFunc<&interval_trunc>, true, false, TArgs_interval_trunc_Policy>; }
struct TArgs_interval_um_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_interval_um() { return &GenericExec<TPgDirectFunc<&interval_um>, true, false, TArgs_interval_um_Policy>; }
struct TArgs_intervaltypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_intervaltypmodin() { return &GenericExec<TPgDirectFunc<&intervaltypmodin>, true, true, TArgs_intervaltypmodin_Policy>; }
struct TArgs_intervaltypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_intervaltypmodout() { return &GenericExec<TPgDirectFunc<&intervaltypmodout>, true, false, TArgs_intervaltypmodout_Policy>; }
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
struct TArgs_jsonb_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_typeof() { return &GenericExec<TPgDirectFunc<&jsonb_typeof>, true, false, TArgs_jsonb_typeof_Policy>; }
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
struct TArgs_likejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_likejoinsel() { return &GenericExec<TPgDirectFunc<&likejoinsel>, true, true, TArgs_likejoinsel_Policy>; }
struct TArgs_likesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_likesel() { return &GenericExec<TPgDirectFunc<&likesel>, true, true, TArgs_likesel_Policy>; }
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
struct TArgs_make_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,true,true,true,true,true};
};
TExecFunc arrow_make_interval() { return &GenericExec<TPgDirectFunc<&make_interval>, true, false, TArgs_make_interval_Policy>; }
struct TArgs_make_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 6> IsFixedArg = {true,true,true,true,true,true};
};
TExecFunc arrow_make_timestamp() { return &GenericExec<TPgDirectFunc<&make_timestamp>, true, true, TArgs_make_timestamp_Policy>; }
struct TArgs_make_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 6> IsFixedArg = {true,true,true,true,true,true};
};
TExecFunc arrow_make_timestamptz() { return &GenericExec<TPgDirectFunc<&make_timestamptz>, true, true, TArgs_make_timestamptz_Policy>; }
struct TArgs_make_timestamptz_at_timezone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,true,true,true,true,false};
};
TExecFunc arrow_make_timestamptz_at_timezone() { return &GenericExec<TPgDirectFunc<&make_timestamptz_at_timezone>, true, true, TArgs_make_timestamptz_at_timezone_Policy>; }
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
struct TArgs_mode_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_mode_final() { return &GenericExec<TPgDirectFunc<&mode_final>, false, true, TArgs_mode_final_Policy>; }
struct TArgs_mul_d_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_mul_d_interval() { return &GenericExec<TPgDirectFunc<&mul_d_interval>, true, false, TArgs_mul_d_interval_Policy>; }
struct TArgs_multirange_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_multirange_typanalyze() { return &GenericExec<TPgDirectFunc<&multirange_typanalyze>, true, true, TArgs_multirange_typanalyze_Policy>; }
struct TArgs_multirangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_multirangesel() { return &GenericExec<TPgDirectFunc<&multirangesel>, true, true, TArgs_multirangesel_Policy>; }
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
struct TArgs_namelike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namelike() { return &GenericExec<TPgDirectFunc<&namelike>, true, true, TArgs_namelike_Policy>; }
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
struct TArgs_namenlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_namenlike() { return &GenericExec<TPgDirectFunc<&namenlike>, true, true, TArgs_namenlike_Policy>; }
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
struct TArgs_namesend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_namesend() { return &GenericExec<TPgDirectFunc<&namesend>, true, false, TArgs_namesend_Policy>; }
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
struct TArgs_nlikejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_nlikejoinsel() { return &GenericExec<TPgDirectFunc<&nlikejoinsel>, true, true, TArgs_nlikejoinsel_Policy>; }
struct TArgs_nlikesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_nlikesel() { return &GenericExec<TPgDirectFunc<&nlikesel>, true, true, TArgs_nlikesel_Policy>; }
struct TArgs_now_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_now() { return &GenericExec<TPgDirectFunc<&now>, true, true, TArgs_now_Policy>; }
struct TArgs_numeric_cash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_cash() { return &GenericExec<TPgDirectFunc<&numeric_cash>, true, true, TArgs_numeric_cash_Policy>; }
struct TArgs_oidtoi8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_oidtoi8() { return &GenericExec<TPgDirectFunc<&oidtoi8>, true, true, TArgs_oidtoi8_Policy>; }
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
struct TArgs_ordered_set_transition_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_ordered_set_transition() { return &GenericExec<TPgDirectFunc<&ordered_set_transition>, false, true, TArgs_ordered_set_transition_Policy>; }
struct TArgs_overlaps_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_overlaps_timestamp() { return &GenericExec<TPgDirectFunc<&overlaps_timestamp>, false, true, TArgs_overlaps_timestamp_Policy>; }
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
struct TArgs_percentile_cont_float8_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_percentile_cont_float8_final() { return &GenericExec<TPgDirectFunc<&percentile_cont_float8_final>, false, true, TArgs_percentile_cont_float8_final_Policy>; }
struct TArgs_percentile_cont_float8_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_percentile_cont_float8_multi_final() { return &GenericExec<TPgDirectFunc<&percentile_cont_float8_multi_final>, false, false, TArgs_percentile_cont_float8_multi_final_Policy>; }
struct TArgs_percentile_cont_interval_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_percentile_cont_interval_final() { return &GenericExec<TPgDirectFunc<&percentile_cont_interval_final>, false, false, TArgs_percentile_cont_interval_final_Policy>; }
struct TArgs_percentile_cont_interval_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_percentile_cont_interval_multi_final() { return &GenericExec<TPgDirectFunc<&percentile_cont_interval_multi_final>, false, false, TArgs_percentile_cont_interval_multi_final_Policy>; }
struct TArgs_percentile_disc_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_percentile_disc_final() { return &GenericExec<TPgDirectFunc<&percentile_disc_final>, false, true, TArgs_percentile_disc_final_Policy>; }
struct TArgs_percentile_disc_multi_final_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_percentile_disc_multi_final() { return &GenericExec<TPgDirectFunc<&percentile_disc_multi_final>, false, false, TArgs_percentile_disc_multi_final_Policy>; }
struct TArgs_pg_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backend_pid() { return &GenericExec<TPgDirectFunc<&pg_backend_pid>, true, true, TArgs_pg_backend_pid_Policy>; }
struct TArgs_pg_backup_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_backup_start_time() { return &GenericExec<TPgDirectFunc<&pg_backup_start_time>, true, true, TArgs_pg_backup_start_time_Policy>; }
struct TArgs_pg_conf_load_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_conf_load_time() { return &GenericExec<TPgDirectFunc<&pg_conf_load_time>, true, true, TArgs_pg_conf_load_time_Policy>; }
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
struct TArgs_pg_get_wal_replay_pause_state_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_get_wal_replay_pause_state() { return &GenericExec<TPgDirectFunc<&pg_get_wal_replay_pause_state>, true, false, TArgs_pg_get_wal_replay_pause_state_Policy>; }
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
struct TArgs_pg_is_wal_replay_paused_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_is_wal_replay_paused() { return &GenericExec<TPgDirectFunc<&pg_is_wal_replay_paused>, true, true, TArgs_pg_is_wal_replay_paused_Policy>; }
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
struct TArgs_pg_partition_root_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_partition_root() { return &GenericExec<TPgDirectFunc<&pg_partition_root>, true, true, TArgs_pg_partition_root_Policy>; }
struct TArgs_pg_postmaster_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_postmaster_start_time() { return &GenericExec<TPgDirectFunc<&pg_postmaster_start_time>, true, true, TArgs_pg_postmaster_start_time_Policy>; }
struct TArgs_pg_promote_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_promote() { return &GenericExec<TPgDirectFunc<&pg_promote>, true, true, TArgs_pg_promote_Policy>; }
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
struct TArgs_pg_start_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_pg_start_backup() { return &GenericExec<TPgDirectFunc<&pg_start_backup>, true, true, TArgs_pg_start_backup_Policy>; }
struct TArgs_pg_stat_clear_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_clear_snapshot() { return &GenericExec<TPgDirectFunc<&pg_stat_clear_snapshot>, false, true, TArgs_pg_stat_clear_snapshot_Policy>; }
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
struct TArgs_prefixjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_prefixjoinsel() { return &GenericExec<TPgDirectFunc<&prefixjoinsel>, true, true, TArgs_prefixjoinsel_Policy>; }
struct TArgs_prefixsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_prefixsel() { return &GenericExec<TPgDirectFunc<&prefixsel>, true, true, TArgs_prefixsel_Policy>; }
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
struct TArgs_regexeqjoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_regexeqjoinsel() { return &GenericExec<TPgDirectFunc<&regexeqjoinsel>, true, true, TArgs_regexeqjoinsel_Policy>; }
struct TArgs_regexeqsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_regexeqsel() { return &GenericExec<TPgDirectFunc<&regexeqsel>, true, true, TArgs_regexeqsel_Policy>; }
struct TArgs_regexnejoinsel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,true,true,true,true};
};
TExecFunc arrow_regexnejoinsel() { return &GenericExec<TPgDirectFunc<&regexnejoinsel>, true, true, TArgs_regexnejoinsel_Policy>; }
struct TArgs_regexnesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_regexnesel() { return &GenericExec<TPgDirectFunc<&regexnesel>, true, true, TArgs_regexnesel_Policy>; }
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
struct TArgs_session_user_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_session_user() { return &GenericExec<TPgDirectFunc<&session_user>, true, false, TArgs_session_user_Policy>; }
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
struct TArgs_spghandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_spghandler() { return &GenericExec<TPgDirectFunc<&spghandler>, true, true, TArgs_spghandler_Policy>; }
struct TArgs_statement_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_statement_timestamp() { return &GenericExec<TPgDirectFunc<&statement_timestamp>, true, true, TArgs_statement_timestamp_Policy>; }
struct TArgs_text_regclass_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_text_regclass() { return &GenericExec<TPgDirectFunc<&text_regclass>, true, true, TArgs_text_regclass_Policy>; }
struct TArgs_texticlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticlike() { return &GenericExec<TPgDirectFunc<&texticlike>, true, true, TArgs_texticlike_Policy>; }
struct TArgs_texticlike_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_texticlike_support() { return &GenericExec<TPgDirectFunc<&texticlike_support>, true, true, TArgs_texticlike_support_Policy>; }
struct TArgs_texticnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticnlike() { return &GenericExec<TPgDirectFunc<&texticnlike>, true, true, TArgs_texticnlike_Policy>; }
struct TArgs_texticregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticregexeq() { return &GenericExec<TPgDirectFunc<&texticregexeq>, true, true, TArgs_texticregexeq_Policy>; }
struct TArgs_texticregexeq_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_texticregexeq_support() { return &GenericExec<TPgDirectFunc<&texticregexeq_support>, true, true, TArgs_texticregexeq_support_Policy>; }
struct TArgs_texticregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_texticregexne() { return &GenericExec<TPgDirectFunc<&texticregexne>, true, true, TArgs_texticregexne_Policy>; }
struct TArgs_textlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textlike() { return &GenericExec<TPgDirectFunc<&textlike>, true, true, TArgs_textlike_Policy>; }
struct TArgs_textlike_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textlike_support() { return &GenericExec<TPgDirectFunc<&textlike_support>, true, true, TArgs_textlike_support_Policy>; }
struct TArgs_textnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textnlike() { return &GenericExec<TPgDirectFunc<&textnlike>, true, true, TArgs_textnlike_Policy>; }
struct TArgs_textregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_textregexeq() { return &GenericExec<TPgDirectFunc<&textregexeq>, true, true, TArgs_textregexeq_Policy>; }
struct TArgs_textregexeq_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textregexeq_support() { return &GenericExec<TPgDirectFunc<&textregexeq_support>, true, true, TArgs_textregexeq_support_Policy>; }
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
struct TArgs_timeofday_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_timeofday() { return &GenericExec<TPgDirectFunc<&timeofday>, true, false, TArgs_timeofday_Policy>; }
struct TArgs_timestamp_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_age() { return &GenericExec<TPgDirectFunc<&timestamp_age>, true, false, TArgs_timestamp_age_Policy>; }
struct TArgs_timestamp_bin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timestamp_bin() { return &GenericExec<TPgDirectFunc<&timestamp_bin>, true, true, TArgs_timestamp_bin_Policy>; }
struct TArgs_timestamp_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_cmp() { return &GenericExec<TPgDirectFunc<&timestamp_cmp>, true, true, TArgs_timestamp_cmp_Policy>; }
struct TArgs_timestamp_cmp_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_cmp_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_cmp_timestamptz>, true, true, TArgs_timestamp_cmp_timestamptz_Policy>; }
struct TArgs_timestamp_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_eq() { return &GenericExec<TPgDirectFunc<&timestamp_eq>, true, true, TArgs_timestamp_eq_Policy>; }
struct TArgs_timestamp_eq_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_eq_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_eq_timestamptz>, true, true, TArgs_timestamp_eq_timestamptz_Policy>; }
struct TArgs_timestamp_finite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_finite() { return &GenericExec<TPgDirectFunc<&timestamp_finite>, true, true, TArgs_timestamp_finite_Policy>; }
struct TArgs_timestamp_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ge() { return &GenericExec<TPgDirectFunc<&timestamp_ge>, true, true, TArgs_timestamp_ge_Policy>; }
struct TArgs_timestamp_ge_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ge_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_ge_timestamptz>, true, true, TArgs_timestamp_ge_timestamptz_Policy>; }
struct TArgs_timestamp_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_gt() { return &GenericExec<TPgDirectFunc<&timestamp_gt>, true, true, TArgs_timestamp_gt_Policy>; }
struct TArgs_timestamp_gt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_gt_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_gt_timestamptz>, true, true, TArgs_timestamp_gt_timestamptz_Policy>; }
struct TArgs_timestamp_hash_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_hash() { return &GenericExec<TPgDirectFunc<&timestamp_hash>, true, true, TArgs_timestamp_hash_Policy>; }
struct TArgs_timestamp_hash_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_hash_extended() { return &GenericExec<TPgDirectFunc<&timestamp_hash_extended>, true, true, TArgs_timestamp_hash_extended_Policy>; }
struct TArgs_timestamp_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timestamp_in() { return &GenericExec<TPgDirectFunc<&timestamp_in>, true, true, TArgs_timestamp_in_Policy>; }
struct TArgs_timestamp_izone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamp_izone() { return &GenericExec<TPgDirectFunc<&timestamp_izone>, true, true, TArgs_timestamp_izone_Policy>; }
struct TArgs_timestamp_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_larger() { return &GenericExec<TPgDirectFunc<&timestamp_larger>, true, true, TArgs_timestamp_larger_Policy>; }
struct TArgs_timestamp_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_le() { return &GenericExec<TPgDirectFunc<&timestamp_le>, true, true, TArgs_timestamp_le_Policy>; }
struct TArgs_timestamp_le_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_le_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_le_timestamptz>, true, true, TArgs_timestamp_le_timestamptz_Policy>; }
struct TArgs_timestamp_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_lt() { return &GenericExec<TPgDirectFunc<&timestamp_lt>, true, true, TArgs_timestamp_lt_Policy>; }
struct TArgs_timestamp_lt_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_lt_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_lt_timestamptz>, true, true, TArgs_timestamp_lt_timestamptz_Policy>; }
struct TArgs_timestamp_mi_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_mi() { return &GenericExec<TPgDirectFunc<&timestamp_mi>, true, false, TArgs_timestamp_mi_Policy>; }
struct TArgs_timestamp_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamp_mi_interval() { return &GenericExec<TPgDirectFunc<&timestamp_mi_interval>, true, true, TArgs_timestamp_mi_interval_Policy>; }
struct TArgs_timestamp_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ne() { return &GenericExec<TPgDirectFunc<&timestamp_ne>, true, true, TArgs_timestamp_ne_Policy>; }
struct TArgs_timestamp_ne_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_ne_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_ne_timestamptz>, true, true, TArgs_timestamp_ne_timestamptz_Policy>; }
struct TArgs_timestamp_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_out() { return &GenericExec<TPgDirectFunc<&timestamp_out>, true, false, TArgs_timestamp_out_Policy>; }
struct TArgs_timestamp_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamp_part() { return &GenericExec<TPgDirectFunc<&timestamp_part>, true, true, TArgs_timestamp_part_Policy>; }
struct TArgs_timestamp_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamp_pl_interval() { return &GenericExec<TPgDirectFunc<&timestamp_pl_interval>, true, true, TArgs_timestamp_pl_interval_Policy>; }
struct TArgs_timestamp_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_timestamp_recv() { return &GenericExec<TPgDirectFunc<&timestamp_recv>, true, true, TArgs_timestamp_recv_Policy>; }
struct TArgs_timestamp_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_scale() { return &GenericExec<TPgDirectFunc<&timestamp_scale>, true, true, TArgs_timestamp_scale_Policy>; }
struct TArgs_timestamp_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_send() { return &GenericExec<TPgDirectFunc<&timestamp_send>, true, false, TArgs_timestamp_send_Policy>; }
struct TArgs_timestamp_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamp_smaller() { return &GenericExec<TPgDirectFunc<&timestamp_smaller>, true, true, TArgs_timestamp_smaller_Policy>; }
struct TArgs_timestamp_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_sortsupport() { return &GenericExec<TPgDirectFunc<&timestamp_sortsupport>, true, true, TArgs_timestamp_sortsupport_Policy>; }
struct TArgs_timestamp_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_support() { return &GenericExec<TPgDirectFunc<&timestamp_support>, true, true, TArgs_timestamp_support_Policy>; }
struct TArgs_timestamp_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamp_timestamptz() { return &GenericExec<TPgDirectFunc<&timestamp_timestamptz>, true, true, TArgs_timestamp_timestamptz_Policy>; }
struct TArgs_timestamp_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamp_trunc() { return &GenericExec<TPgDirectFunc<&timestamp_trunc>, true, true, TArgs_timestamp_trunc_Policy>; }
struct TArgs_timestamp_zone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamp_zone() { return &GenericExec<TPgDirectFunc<&timestamp_zone>, true, true, TArgs_timestamp_zone_Policy>; }
struct TArgs_timestamptypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timestamptypmodin() { return &GenericExec<TPgDirectFunc<&timestamptypmodin>, true, true, TArgs_timestamptypmodin_Policy>; }
struct TArgs_timestamptypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptypmodout() { return &GenericExec<TPgDirectFunc<&timestamptypmodout>, true, false, TArgs_timestamptypmodout_Policy>; }
struct TArgs_timestamptz_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_age() { return &GenericExec<TPgDirectFunc<&timestamptz_age>, true, false, TArgs_timestamptz_age_Policy>; }
struct TArgs_timestamptz_bin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timestamptz_bin() { return &GenericExec<TPgDirectFunc<&timestamptz_bin>, true, true, TArgs_timestamptz_bin_Policy>; }
struct TArgs_timestamptz_cmp_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_cmp_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_cmp_timestamp>, true, true, TArgs_timestamptz_cmp_timestamp_Policy>; }
struct TArgs_timestamptz_eq_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_eq_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_eq_timestamp>, true, true, TArgs_timestamptz_eq_timestamp_Policy>; }
struct TArgs_timestamptz_ge_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ge_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_ge_timestamp>, true, true, TArgs_timestamptz_ge_timestamp_Policy>; }
struct TArgs_timestamptz_gt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_gt_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_gt_timestamp>, true, true, TArgs_timestamptz_gt_timestamp_Policy>; }
struct TArgs_timestamptz_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_timestamptz_in() { return &GenericExec<TPgDirectFunc<&timestamptz_in>, true, true, TArgs_timestamptz_in_Policy>; }
struct TArgs_timestamptz_izone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamptz_izone() { return &GenericExec<TPgDirectFunc<&timestamptz_izone>, true, true, TArgs_timestamptz_izone_Policy>; }
struct TArgs_timestamptz_le_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_le_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_le_timestamp>, true, true, TArgs_timestamptz_le_timestamp_Policy>; }
struct TArgs_timestamptz_lt_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_lt_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_lt_timestamp>, true, true, TArgs_timestamptz_lt_timestamp_Policy>; }
struct TArgs_timestamptz_mi_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamptz_mi_interval() { return &GenericExec<TPgDirectFunc<&timestamptz_mi_interval>, true, true, TArgs_timestamptz_mi_interval_Policy>; }
struct TArgs_timestamptz_ne_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_ne_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_ne_timestamp>, true, true, TArgs_timestamptz_ne_timestamp_Policy>; }
struct TArgs_timestamptz_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_out() { return &GenericExec<TPgDirectFunc<&timestamptz_out>, true, false, TArgs_timestamptz_out_Policy>; }
struct TArgs_timestamptz_part_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamptz_part() { return &GenericExec<TPgDirectFunc<&timestamptz_part>, true, true, TArgs_timestamptz_part_Policy>; }
struct TArgs_timestamptz_pl_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_timestamptz_pl_interval() { return &GenericExec<TPgDirectFunc<&timestamptz_pl_interval>, true, true, TArgs_timestamptz_pl_interval_Policy>; }
struct TArgs_timestamptz_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_timestamptz_recv() { return &GenericExec<TPgDirectFunc<&timestamptz_recv>, true, true, TArgs_timestamptz_recv_Policy>; }
struct TArgs_timestamptz_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_timestamptz_scale() { return &GenericExec<TPgDirectFunc<&timestamptz_scale>, true, true, TArgs_timestamptz_scale_Policy>; }
struct TArgs_timestamptz_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_send() { return &GenericExec<TPgDirectFunc<&timestamptz_send>, true, false, TArgs_timestamptz_send_Policy>; }
struct TArgs_timestamptz_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptz_timestamp() { return &GenericExec<TPgDirectFunc<&timestamptz_timestamp>, true, true, TArgs_timestamptz_timestamp_Policy>; }
struct TArgs_timestamptz_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamptz_trunc() { return &GenericExec<TPgDirectFunc<&timestamptz_trunc>, true, true, TArgs_timestamptz_trunc_Policy>; }
struct TArgs_timestamptz_trunc_zone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
TExecFunc arrow_timestamptz_trunc_zone() { return &GenericExec<TPgDirectFunc<&timestamptz_trunc_zone>, true, true, TArgs_timestamptz_trunc_zone_Policy>; }
struct TArgs_timestamptz_zone_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_timestamptz_zone() { return &GenericExec<TPgDirectFunc<&timestamptz_zone>, true, true, TArgs_timestamptz_zone_Policy>; }
struct TArgs_timestamptztypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_timestamptztypmodin() { return &GenericExec<TPgDirectFunc<&timestamptztypmodin>, true, true, TArgs_timestamptztypmodin_Policy>; }
struct TArgs_timestamptztypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_timestamptztypmodout() { return &GenericExec<TPgDirectFunc<&timestamptztypmodout>, true, false, TArgs_timestamptztypmodout_Policy>; }
struct TArgs_to_jsonb_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_jsonb() { return &GenericExec<TPgDirectFunc<&to_jsonb>, true, false, TArgs_to_jsonb_Policy>; }
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
