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
struct TArgs_amvalidate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_amvalidate() { return &GenericExec<TPgDirectFunc<&amvalidate>, true, true, TArgs_amvalidate_Policy>; }
struct TArgs_any_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_any_in() { return &GenericExec<TPgDirectFunc<&any_in>, true, true, TArgs_any_in_Policy>; }
struct TArgs_any_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_any_out() { return &GenericExec<TPgDirectFunc<&any_out>, true, false, TArgs_any_out_Policy>; }
struct TArgs_anyarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_in() { return &GenericExec<TPgDirectFunc<&anyarray_in>, true, false, TArgs_anyarray_in_Policy>; }
struct TArgs_anyarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_out() { return &GenericExec<TPgDirectFunc<&anyarray_out>, true, false, TArgs_anyarray_out_Policy>; }
struct TArgs_anyarray_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyarray_recv() { return &GenericExec<TPgDirectFunc<&anyarray_recv>, true, false, TArgs_anyarray_recv_Policy>; }
struct TArgs_anyarray_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyarray_send() { return &GenericExec<TPgDirectFunc<&anyarray_send>, true, false, TArgs_anyarray_send_Policy>; }
struct TArgs_anycompatible_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatible_in() { return &GenericExec<TPgDirectFunc<&anycompatible_in>, true, true, TArgs_anycompatible_in_Policy>; }
struct TArgs_anycompatible_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatible_out() { return &GenericExec<TPgDirectFunc<&anycompatible_out>, true, false, TArgs_anycompatible_out_Policy>; }
struct TArgs_anycompatiblearray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_in() { return &GenericExec<TPgDirectFunc<&anycompatiblearray_in>, true, false, TArgs_anycompatiblearray_in_Policy>; }
struct TArgs_anycompatiblearray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_out() { return &GenericExec<TPgDirectFunc<&anycompatiblearray_out>, true, false, TArgs_anycompatiblearray_out_Policy>; }
struct TArgs_anycompatiblearray_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatiblearray_recv() { return &GenericExec<TPgDirectFunc<&anycompatiblearray_recv>, true, false, TArgs_anycompatiblearray_recv_Policy>; }
struct TArgs_anycompatiblearray_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblearray_send() { return &GenericExec<TPgDirectFunc<&anycompatiblearray_send>, true, false, TArgs_anycompatiblearray_send_Policy>; }
struct TArgs_anycompatiblemultirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anycompatiblemultirange_in() { return &GenericExec<TPgDirectFunc<&anycompatiblemultirange_in>, true, false, TArgs_anycompatiblemultirange_in_Policy>; }
struct TArgs_anycompatiblemultirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblemultirange_out() { return &GenericExec<TPgDirectFunc<&anycompatiblemultirange_out>, true, false, TArgs_anycompatiblemultirange_out_Policy>; }
struct TArgs_anycompatiblenonarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblenonarray_in() { return &GenericExec<TPgDirectFunc<&anycompatiblenonarray_in>, true, true, TArgs_anycompatiblenonarray_in_Policy>; }
struct TArgs_anycompatiblenonarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anycompatiblenonarray_out() { return &GenericExec<TPgDirectFunc<&anycompatiblenonarray_out>, true, false, TArgs_anycompatiblenonarray_out_Policy>; }
struct TArgs_anycompatiblerange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anycompatiblerange_in() { return &GenericExec<TPgDirectFunc<&anycompatiblerange_in>, true, false, TArgs_anycompatiblerange_in_Policy>; }
struct TArgs_anycompatiblerange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anycompatiblerange_out() { return &GenericExec<TPgDirectFunc<&anycompatiblerange_out>, true, false, TArgs_anycompatiblerange_out_Policy>; }
struct TArgs_anyelement_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyelement_in() { return &GenericExec<TPgDirectFunc<&anyelement_in>, true, true, TArgs_anyelement_in_Policy>; }
struct TArgs_anyelement_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyelement_out() { return &GenericExec<TPgDirectFunc<&anyelement_out>, true, false, TArgs_anyelement_out_Policy>; }
struct TArgs_anyenum_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyenum_in() { return &GenericExec<TPgDirectFunc<&anyenum_in>, true, true, TArgs_anyenum_in_Policy>; }
struct TArgs_anyenum_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anyenum_out() { return &GenericExec<TPgDirectFunc<&anyenum_out>, true, false, TArgs_anyenum_out_Policy>; }
struct TArgs_anymultirange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anymultirange_in() { return &GenericExec<TPgDirectFunc<&anymultirange_in>, true, false, TArgs_anymultirange_in_Policy>; }
struct TArgs_anymultirange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anymultirange_out() { return &GenericExec<TPgDirectFunc<&anymultirange_out>, true, false, TArgs_anymultirange_out_Policy>; }
struct TArgs_anynonarray_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anynonarray_in() { return &GenericExec<TPgDirectFunc<&anynonarray_in>, true, true, TArgs_anynonarray_in_Policy>; }
struct TArgs_anynonarray_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_anynonarray_out() { return &GenericExec<TPgDirectFunc<&anynonarray_out>, true, false, TArgs_anynonarray_out_Policy>; }
struct TArgs_anyrange_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_anyrange_in() { return &GenericExec<TPgDirectFunc<&anyrange_in>, true, false, TArgs_anyrange_in_Policy>; }
struct TArgs_anyrange_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_anyrange_out() { return &GenericExec<TPgDirectFunc<&anyrange_out>, true, false, TArgs_anyrange_out_Policy>; }
struct TArgs_array_cardinality_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_cardinality() { return &GenericExec<TPgDirectFunc<&array_cardinality>, true, true, TArgs_array_cardinality_Policy>; }
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
struct TArgs_bit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bit() { return &GenericExec<TPgDirectFunc<&bit>, true, false, TArgs_bit_Policy>; }
struct TArgs_bit_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bit_and() { return &GenericExec<TPgDirectFunc<&bit_and>, true, false, TArgs_bit_and_Policy>; }
struct TArgs_bit_bit_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bit_bit_count() { return &GenericExec<TPgDirectFunc<&bit_bit_count>, true, true, TArgs_bit_bit_count_Policy>; }
struct TArgs_bit_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bit_in() { return &GenericExec<TPgDirectFunc<&bit_in>, true, false, TArgs_bit_in_Policy>; }
struct TArgs_bit_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bit_or() { return &GenericExec<TPgDirectFunc<&bit_or>, true, false, TArgs_bit_or_Policy>; }
struct TArgs_bit_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bit_out() { return &GenericExec<TPgDirectFunc<&bit_out>, true, false, TArgs_bit_out_Policy>; }
struct TArgs_bit_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_bit_recv() { return &GenericExec<TPgDirectFunc<&bit_recv>, true, false, TArgs_bit_recv_Policy>; }
struct TArgs_bit_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bit_send() { return &GenericExec<TPgDirectFunc<&bit_send>, true, false, TArgs_bit_send_Policy>; }
struct TArgs_bitcat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitcat() { return &GenericExec<TPgDirectFunc<&bitcat>, true, false, TArgs_bitcat_Policy>; }
struct TArgs_bitcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitcmp() { return &GenericExec<TPgDirectFunc<&bitcmp>, true, true, TArgs_bitcmp_Policy>; }
struct TArgs_biteq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_biteq() { return &GenericExec<TPgDirectFunc<&biteq>, true, true, TArgs_biteq_Policy>; }
struct TArgs_bitfromint4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_bitfromint4() { return &GenericExec<TPgDirectFunc<&bitfromint4>, true, false, TArgs_bitfromint4_Policy>; }
struct TArgs_bitfromint8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_bitfromint8() { return &GenericExec<TPgDirectFunc<&bitfromint8>, true, false, TArgs_bitfromint8_Policy>; }
struct TArgs_bitge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitge() { return &GenericExec<TPgDirectFunc<&bitge>, true, true, TArgs_bitge_Policy>; }
struct TArgs_bitgetbit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bitgetbit() { return &GenericExec<TPgDirectFunc<&bitgetbit>, true, true, TArgs_bitgetbit_Policy>; }
struct TArgs_bitgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitgt() { return &GenericExec<TPgDirectFunc<&bitgt>, true, true, TArgs_bitgt_Policy>; }
struct TArgs_bitle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitle() { return &GenericExec<TPgDirectFunc<&bitle>, true, true, TArgs_bitle_Policy>; }
struct TArgs_bitlength_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bitlength() { return &GenericExec<TPgDirectFunc<&bitlength>, true, true, TArgs_bitlength_Policy>; }
struct TArgs_bitlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitlt() { return &GenericExec<TPgDirectFunc<&bitlt>, true, true, TArgs_bitlt_Policy>; }
struct TArgs_bitne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitne() { return &GenericExec<TPgDirectFunc<&bitne>, true, true, TArgs_bitne_Policy>; }
struct TArgs_bitnot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bitnot() { return &GenericExec<TPgDirectFunc<&bitnot>, true, false, TArgs_bitnot_Policy>; }
struct TArgs_bitoctetlength_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bitoctetlength() { return &GenericExec<TPgDirectFunc<&bitoctetlength>, true, true, TArgs_bitoctetlength_Policy>; }
struct TArgs_bitoverlay_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,true,true};
};
TExecFunc arrow_bitoverlay() { return &GenericExec<TPgDirectFunc<&bitoverlay>, true, false, TArgs_bitoverlay_Policy>; }
struct TArgs_bitoverlay_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_bitoverlay_no_len() { return &GenericExec<TPgDirectFunc<&bitoverlay_no_len>, true, false, TArgs_bitoverlay_no_len_Policy>; }
struct TArgs_bitposition_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitposition() { return &GenericExec<TPgDirectFunc<&bitposition>, true, true, TArgs_bitposition_Policy>; }
struct TArgs_bitsetbit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bitsetbit() { return &GenericExec<TPgDirectFunc<&bitsetbit>, true, false, TArgs_bitsetbit_Policy>; }
struct TArgs_bitshiftleft_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bitshiftleft() { return &GenericExec<TPgDirectFunc<&bitshiftleft>, true, false, TArgs_bitshiftleft_Policy>; }
struct TArgs_bitshiftright_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bitshiftright() { return &GenericExec<TPgDirectFunc<&bitshiftright>, true, false, TArgs_bitshiftright_Policy>; }
struct TArgs_bitsubstr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_bitsubstr() { return &GenericExec<TPgDirectFunc<&bitsubstr>, true, false, TArgs_bitsubstr_Policy>; }
struct TArgs_bitsubstr_no_len_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_bitsubstr_no_len() { return &GenericExec<TPgDirectFunc<&bitsubstr_no_len>, true, false, TArgs_bitsubstr_no_len_Policy>; }
struct TArgs_bittoint4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bittoint4() { return &GenericExec<TPgDirectFunc<&bittoint4>, true, true, TArgs_bittoint4_Policy>; }
struct TArgs_bittoint8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bittoint8() { return &GenericExec<TPgDirectFunc<&bittoint8>, true, true, TArgs_bittoint8_Policy>; }
struct TArgs_bittypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_bittypmodin() { return &GenericExec<TPgDirectFunc<&bittypmodin>, true, true, TArgs_bittypmodin_Policy>; }
struct TArgs_bittypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_bittypmodout() { return &GenericExec<TPgDirectFunc<&bittypmodout>, true, false, TArgs_bittypmodout_Policy>; }
struct TArgs_bitxor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bitxor() { return &GenericExec<TPgDirectFunc<&bitxor>, true, false, TArgs_bitxor_Policy>; }
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
struct TArgs_btnametextcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_btnametextcmp() { return &GenericExec<TPgDirectFunc<&btnametextcmp>, true, true, TArgs_btnametextcmp_Policy>; }
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
struct TArgs_bytealtrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_bytealtrim() { return &GenericExec<TPgDirectFunc<&bytealtrim>, true, false, TArgs_bytealtrim_Policy>; }
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
struct TArgs_byteartrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteartrim() { return &GenericExec<TPgDirectFunc<&byteartrim>, true, false, TArgs_byteartrim_Policy>; }
struct TArgs_byteasend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_byteasend() { return &GenericExec<TPgDirectFunc<&byteasend>, true, false, TArgs_byteasend_Policy>; }
struct TArgs_byteatrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_byteatrim() { return &GenericExec<TPgDirectFunc<&byteatrim>, true, false, TArgs_byteatrim_Policy>; }
struct TArgs_chr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_chr() { return &GenericExec<TPgDirectFunc<&chr>, true, false, TArgs_chr_Policy>; }
struct TArgs_cstring_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_in() { return &GenericExec<TPgDirectFunc<&cstring_in>, true, false, TArgs_cstring_in_Policy>; }
struct TArgs_cstring_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_out() { return &GenericExec<TPgDirectFunc<&cstring_out>, true, false, TArgs_cstring_out_Policy>; }
struct TArgs_cstring_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_cstring_recv() { return &GenericExec<TPgDirectFunc<&cstring_recv>, true, false, TArgs_cstring_recv_Policy>; }
struct TArgs_cstring_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_cstring_send() { return &GenericExec<TPgDirectFunc<&cstring_send>, true, false, TArgs_cstring_send_Policy>; }
struct TArgs_elem_contained_by_multirange_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_elem_contained_by_multirange() { return &GenericExec<TPgDirectFunc<&elem_contained_by_multirange>, true, true, TArgs_elem_contained_by_multirange_Policy>; }
struct TArgs_event_trigger_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_event_trigger_in() { return &GenericExec<TPgDirectFunc<&event_trigger_in>, false, true, TArgs_event_trigger_in_Policy>; }
struct TArgs_event_trigger_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_event_trigger_out() { return &GenericExec<TPgDirectFunc<&event_trigger_out>, true, false, TArgs_event_trigger_out_Policy>; }
struct TArgs_fdw_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_fdw_handler_in() { return &GenericExec<TPgDirectFunc<&fdw_handler_in>, false, true, TArgs_fdw_handler_in_Policy>; }
struct TArgs_fdw_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_fdw_handler_out() { return &GenericExec<TPgDirectFunc<&fdw_handler_out>, true, false, TArgs_fdw_handler_out_Policy>; }
struct TArgs_float4_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float4_numeric() { return &GenericExec<TPgDirectFunc<&float4_numeric>, true, false, TArgs_float4_numeric_Policy>; }
struct TArgs_float8_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8_numeric() { return &GenericExec<TPgDirectFunc<&float8_numeric>, true, false, TArgs_float8_numeric_Policy>; }
struct TArgs_get_current_ts_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_get_current_ts_config() { return &GenericExec<TPgDirectFunc<&get_current_ts_config>, true, true, TArgs_get_current_ts_config_Policy>; }
struct TArgs_getdatabaseencoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_getdatabaseencoding() { return &GenericExec<TPgDirectFunc<&getdatabaseencoding>, true, false, TArgs_getdatabaseencoding_Policy>; }
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
struct TArgs_ginqueryarrayextract_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
TExecFunc arrow_ginqueryarrayextract() { return &GenericExec<TPgDirectFunc<&ginqueryarrayextract>, true, true, TArgs_ginqueryarrayextract_Policy>; }
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
struct TArgs_gisthandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gisthandler() { return &GenericExec<TPgDirectFunc<&gisthandler>, true, true, TArgs_gisthandler_Policy>; }
struct TArgs_gtsvector_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gtsvector_compress() { return &GenericExec<TPgDirectFunc<&gtsvector_compress>, true, true, TArgs_gtsvector_compress_Policy>; }
struct TArgs_gtsvector_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gtsvector_consistent() { return &GenericExec<TPgDirectFunc<&gtsvector_consistent>, true, true, TArgs_gtsvector_consistent_Policy>; }
struct TArgs_gtsvector_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
TExecFunc arrow_gtsvector_consistent_oldsig() { return &GenericExec<TPgDirectFunc<&gtsvector_consistent_oldsig>, true, true, TArgs_gtsvector_consistent_oldsig_Policy>; }
struct TArgs_gtsvector_decompress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gtsvector_decompress() { return &GenericExec<TPgDirectFunc<&gtsvector_decompress>, true, true, TArgs_gtsvector_decompress_Policy>; }
struct TArgs_gtsvector_options_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_gtsvector_options() { return &GenericExec<TPgDirectFunc<&gtsvector_options>, false, true, TArgs_gtsvector_options_Policy>; }
struct TArgs_gtsvector_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_gtsvector_penalty() { return &GenericExec<TPgDirectFunc<&gtsvector_penalty>, true, true, TArgs_gtsvector_penalty_Policy>; }
struct TArgs_gtsvector_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsvector_picksplit() { return &GenericExec<TPgDirectFunc<&gtsvector_picksplit>, true, true, TArgs_gtsvector_picksplit_Policy>; }
struct TArgs_gtsvector_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_gtsvector_same() { return &GenericExec<TPgDirectFunc<&gtsvector_same>, true, true, TArgs_gtsvector_same_Policy>; }
struct TArgs_gtsvector_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_gtsvector_union() { return &GenericExec<TPgDirectFunc<&gtsvector_union>, true, false, TArgs_gtsvector_union_Policy>; }
struct TArgs_gtsvectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_gtsvectorin() { return &GenericExec<TPgDirectFunc<&gtsvectorin>, true, false, TArgs_gtsvectorin_Policy>; }
struct TArgs_gtsvectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_gtsvectorout() { return &GenericExec<TPgDirectFunc<&gtsvectorout>, true, false, TArgs_gtsvectorout_Policy>; }
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
struct TArgs_hash_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hash_numeric() { return &GenericExec<TPgDirectFunc<&hash_numeric>, true, true, TArgs_hash_numeric_Policy>; }
struct TArgs_hash_numeric_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hash_numeric_extended() { return &GenericExec<TPgDirectFunc<&hash_numeric_extended>, true, true, TArgs_hash_numeric_extended_Policy>; }
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
struct TArgs_hashmacaddr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_hashmacaddr() { return &GenericExec<TPgDirectFunc<&hashmacaddr>, true, true, TArgs_hashmacaddr_Policy>; }
struct TArgs_hashmacaddrextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_hashmacaddrextended() { return &GenericExec<TPgDirectFunc<&hashmacaddrextended>, true, true, TArgs_hashmacaddrextended_Policy>; }
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
struct TArgs_in_range_numeric_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_numeric_numeric() { return &GenericExec<TPgDirectFunc<&in_range_numeric_numeric>, true, true, TArgs_in_range_numeric_numeric_Policy>; }
struct TArgs_index_am_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_index_am_handler_in() { return &GenericExec<TPgDirectFunc<&index_am_handler_in>, false, true, TArgs_index_am_handler_in_Policy>; }
struct TArgs_index_am_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_index_am_handler_out() { return &GenericExec<TPgDirectFunc<&index_am_handler_out>, true, false, TArgs_index_am_handler_out_Policy>; }
struct TArgs_initcap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_initcap() { return &GenericExec<TPgDirectFunc<&initcap>, true, false, TArgs_initcap_Policy>; }
struct TArgs_int2_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2_accum() { return &GenericExec<TPgDirectFunc<&int2_accum>, false, true, TArgs_int2_accum_Policy>; }
struct TArgs_int2_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2_accum_inv() { return &GenericExec<TPgDirectFunc<&int2_accum_inv>, false, true, TArgs_int2_accum_inv_Policy>; }
struct TArgs_int2_avg_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int2_avg_accum() { return &GenericExec<TPgDirectFunc<&int2_avg_accum>, true, false, TArgs_int2_avg_accum_Policy>; }
struct TArgs_int2_avg_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int2_avg_accum_inv() { return &GenericExec<TPgDirectFunc<&int2_avg_accum_inv>, true, false, TArgs_int2_avg_accum_inv_Policy>; }
struct TArgs_int2_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int2_numeric() { return &GenericExec<TPgDirectFunc<&int2_numeric>, true, false, TArgs_int2_numeric_Policy>; }
struct TArgs_int2_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int2_sum() { return &GenericExec<TPgDirectFunc<&int2_sum>, false, true, TArgs_int2_sum_Policy>; }
struct TArgs_int2int4_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2int4_sum() { return &GenericExec<TPgDirectFunc<&int2int4_sum>, true, true, TArgs_int2int4_sum_Policy>; }
struct TArgs_int4_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4_accum() { return &GenericExec<TPgDirectFunc<&int4_accum>, false, true, TArgs_int4_accum_Policy>; }
struct TArgs_int4_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4_accum_inv() { return &GenericExec<TPgDirectFunc<&int4_accum_inv>, false, true, TArgs_int4_accum_inv_Policy>; }
struct TArgs_int4_avg_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int4_avg_accum() { return &GenericExec<TPgDirectFunc<&int4_avg_accum>, true, false, TArgs_int4_avg_accum_Policy>; }
struct TArgs_int4_avg_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int4_avg_accum_inv() { return &GenericExec<TPgDirectFunc<&int4_avg_accum_inv>, true, false, TArgs_int4_avg_accum_inv_Policy>; }
struct TArgs_int4_avg_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_int4_avg_combine() { return &GenericExec<TPgDirectFunc<&int4_avg_combine>, true, false, TArgs_int4_avg_combine_Policy>; }
struct TArgs_int4_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4_numeric() { return &GenericExec<TPgDirectFunc<&int4_numeric>, true, false, TArgs_int4_numeric_Policy>; }
struct TArgs_int4_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int4_sum() { return &GenericExec<TPgDirectFunc<&int4_sum>, false, true, TArgs_int4_sum_Policy>; }
struct TArgs_int8_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_accum() { return &GenericExec<TPgDirectFunc<&int8_accum>, false, true, TArgs_int8_accum_Policy>; }
struct TArgs_int8_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_accum_inv() { return &GenericExec<TPgDirectFunc<&int8_accum_inv>, false, true, TArgs_int8_accum_inv_Policy>; }
struct TArgs_int8_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int8_avg() { return &GenericExec<TPgDirectFunc<&int8_avg>, true, false, TArgs_int8_avg_Policy>; }
struct TArgs_int8_avg_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_avg_accum() { return &GenericExec<TPgDirectFunc<&int8_avg_accum>, false, true, TArgs_int8_avg_accum_Policy>; }
struct TArgs_int8_avg_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_avg_accum_inv() { return &GenericExec<TPgDirectFunc<&int8_avg_accum_inv>, false, true, TArgs_int8_avg_accum_inv_Policy>; }
struct TArgs_int8_avg_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_int8_avg_combine() { return &GenericExec<TPgDirectFunc<&int8_avg_combine>, false, true, TArgs_int8_avg_combine_Policy>; }
struct TArgs_int8_avg_deserialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int8_avg_deserialize() { return &GenericExec<TPgDirectFunc<&int8_avg_deserialize>, true, true, TArgs_int8_avg_deserialize_Policy>; }
struct TArgs_int8_avg_serialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8_avg_serialize() { return &GenericExec<TPgDirectFunc<&int8_avg_serialize>, true, false, TArgs_int8_avg_serialize_Policy>; }
struct TArgs_int8_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int8_numeric() { return &GenericExec<TPgDirectFunc<&int8_numeric>, true, false, TArgs_int8_numeric_Policy>; }
struct TArgs_int8_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_int8_sum() { return &GenericExec<TPgDirectFunc<&int8_sum>, false, false, TArgs_int8_sum_Policy>; }
struct TArgs_internal_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_internal_in() { return &GenericExec<TPgDirectFunc<&internal_in>, false, true, TArgs_internal_in_Policy>; }
struct TArgs_internal_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_internal_out() { return &GenericExec<TPgDirectFunc<&internal_out>, true, false, TArgs_internal_out_Policy>; }
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
struct TArgs_json_populate_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_json_populate_record() { return &GenericExec<TPgDirectFunc<&json_populate_record>, false, true, TArgs_json_populate_record_Policy>; }
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
struct TArgs_json_string_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_string_to_tsvector() { return &GenericExec<TPgDirectFunc<&json_string_to_tsvector>, true, false, TArgs_json_string_to_tsvector_Policy>; }
struct TArgs_json_string_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_json_string_to_tsvector_byid() { return &GenericExec<TPgDirectFunc<&json_string_to_tsvector_byid>, true, false, TArgs_json_string_to_tsvector_byid_Policy>; }
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
struct TArgs_json_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_json_to_tsvector() { return &GenericExec<TPgDirectFunc<&json_to_tsvector>, true, false, TArgs_json_to_tsvector_Policy>; }
struct TArgs_json_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_json_to_tsvector_byid() { return &GenericExec<TPgDirectFunc<&json_to_tsvector_byid>, true, false, TArgs_json_to_tsvector_byid_Policy>; }
struct TArgs_json_typeof_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_json_typeof() { return &GenericExec<TPgDirectFunc<&json_typeof>, true, false, TArgs_json_typeof_Policy>; }
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
struct TArgs_jsonb_string_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_jsonb_string_to_tsvector() { return &GenericExec<TPgDirectFunc<&jsonb_string_to_tsvector>, true, false, TArgs_jsonb_string_to_tsvector_Policy>; }
struct TArgs_jsonb_string_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_jsonb_string_to_tsvector_byid() { return &GenericExec<TPgDirectFunc<&jsonb_string_to_tsvector_byid>, true, false, TArgs_jsonb_string_to_tsvector_byid_Policy>; }
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
struct TArgs_jsonb_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_jsonb_to_tsvector() { return &GenericExec<TPgDirectFunc<&jsonb_to_tsvector>, true, false, TArgs_jsonb_to_tsvector_Policy>; }
struct TArgs_jsonb_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_jsonb_to_tsvector_byid() { return &GenericExec<TPgDirectFunc<&jsonb_to_tsvector_byid>, true, false, TArgs_jsonb_to_tsvector_byid_Policy>; }
struct TArgs_language_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_language_handler_in() { return &GenericExec<TPgDirectFunc<&language_handler_in>, false, true, TArgs_language_handler_in_Policy>; }
struct TArgs_language_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_language_handler_out() { return &GenericExec<TPgDirectFunc<&language_handler_out>, true, false, TArgs_language_handler_out_Policy>; }
struct TArgs_length_in_encoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_length_in_encoding() { return &GenericExec<TPgDirectFunc<&length_in_encoding>, true, true, TArgs_length_in_encoding_Policy>; }
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
struct TArgs_macaddr_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_and() { return &GenericExec<TPgDirectFunc<&macaddr_and>, true, false, TArgs_macaddr_and_Policy>; }
struct TArgs_macaddr_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_cmp() { return &GenericExec<TPgDirectFunc<&macaddr_cmp>, true, true, TArgs_macaddr_cmp_Policy>; }
struct TArgs_macaddr_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_eq() { return &GenericExec<TPgDirectFunc<&macaddr_eq>, true, true, TArgs_macaddr_eq_Policy>; }
struct TArgs_macaddr_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_ge() { return &GenericExec<TPgDirectFunc<&macaddr_ge>, true, true, TArgs_macaddr_ge_Policy>; }
struct TArgs_macaddr_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_gt() { return &GenericExec<TPgDirectFunc<&macaddr_gt>, true, true, TArgs_macaddr_gt_Policy>; }
struct TArgs_macaddr_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_in() { return &GenericExec<TPgDirectFunc<&macaddr_in>, true, false, TArgs_macaddr_in_Policy>; }
struct TArgs_macaddr_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_le() { return &GenericExec<TPgDirectFunc<&macaddr_le>, true, true, TArgs_macaddr_le_Policy>; }
struct TArgs_macaddr_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_lt() { return &GenericExec<TPgDirectFunc<&macaddr_lt>, true, true, TArgs_macaddr_lt_Policy>; }
struct TArgs_macaddr_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_ne() { return &GenericExec<TPgDirectFunc<&macaddr_ne>, true, true, TArgs_macaddr_ne_Policy>; }
struct TArgs_macaddr_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_not() { return &GenericExec<TPgDirectFunc<&macaddr_not>, true, false, TArgs_macaddr_not_Policy>; }
struct TArgs_macaddr_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_macaddr_or() { return &GenericExec<TPgDirectFunc<&macaddr_or>, true, false, TArgs_macaddr_or_Policy>; }
struct TArgs_macaddr_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_out() { return &GenericExec<TPgDirectFunc<&macaddr_out>, true, false, TArgs_macaddr_out_Policy>; }
struct TArgs_macaddr_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_macaddr_recv() { return &GenericExec<TPgDirectFunc<&macaddr_recv>, true, false, TArgs_macaddr_recv_Policy>; }
struct TArgs_macaddr_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_send() { return &GenericExec<TPgDirectFunc<&macaddr_send>, true, false, TArgs_macaddr_send_Policy>; }
struct TArgs_macaddr_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_macaddr_sortsupport() { return &GenericExec<TPgDirectFunc<&macaddr_sortsupport>, true, true, TArgs_macaddr_sortsupport_Policy>; }
struct TArgs_macaddr_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_macaddr_trunc() { return &GenericExec<TPgDirectFunc<&macaddr_trunc>, true, false, TArgs_macaddr_trunc_Policy>; }
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
struct TArgs_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric() { return &GenericExec<TPgDirectFunc<&numeric>, true, false, TArgs_numeric_Policy>; }
struct TArgs_numeric_abs_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_abs() { return &GenericExec<TPgDirectFunc<&numeric_abs>, true, false, TArgs_numeric_abs_Policy>; }
struct TArgs_numeric_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_numeric_accum() { return &GenericExec<TPgDirectFunc<&numeric_accum>, false, true, TArgs_numeric_accum_Policy>; }
struct TArgs_numeric_accum_inv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_numeric_accum_inv() { return &GenericExec<TPgDirectFunc<&numeric_accum_inv>, false, true, TArgs_numeric_accum_inv_Policy>; }
struct TArgs_numeric_add_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_add() { return &GenericExec<TPgDirectFunc<&numeric_add>, true, false, TArgs_numeric_add_Policy>; }
struct TArgs_numeric_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_avg() { return &GenericExec<TPgDirectFunc<&numeric_avg>, false, false, TArgs_numeric_avg_Policy>; }
struct TArgs_numeric_avg_accum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_numeric_avg_accum() { return &GenericExec<TPgDirectFunc<&numeric_avg_accum>, false, true, TArgs_numeric_avg_accum_Policy>; }
struct TArgs_numeric_avg_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_numeric_avg_combine() { return &GenericExec<TPgDirectFunc<&numeric_avg_combine>, false, true, TArgs_numeric_avg_combine_Policy>; }
struct TArgs_numeric_avg_deserialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric_avg_deserialize() { return &GenericExec<TPgDirectFunc<&numeric_avg_deserialize>, true, true, TArgs_numeric_avg_deserialize_Policy>; }
struct TArgs_numeric_avg_serialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_avg_serialize() { return &GenericExec<TPgDirectFunc<&numeric_avg_serialize>, true, false, TArgs_numeric_avg_serialize_Policy>; }
struct TArgs_numeric_ceil_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_ceil() { return &GenericExec<TPgDirectFunc<&numeric_ceil>, true, false, TArgs_numeric_ceil_Policy>; }
struct TArgs_numeric_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_cmp() { return &GenericExec<TPgDirectFunc<&numeric_cmp>, true, true, TArgs_numeric_cmp_Policy>; }
struct TArgs_numeric_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_numeric_combine() { return &GenericExec<TPgDirectFunc<&numeric_combine>, false, true, TArgs_numeric_combine_Policy>; }
struct TArgs_numeric_deserialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric_deserialize() { return &GenericExec<TPgDirectFunc<&numeric_deserialize>, true, true, TArgs_numeric_deserialize_Policy>; }
struct TArgs_numeric_div_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_div() { return &GenericExec<TPgDirectFunc<&numeric_div>, true, false, TArgs_numeric_div_Policy>; }
struct TArgs_numeric_div_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_div_trunc() { return &GenericExec<TPgDirectFunc<&numeric_div_trunc>, true, false, TArgs_numeric_div_trunc_Policy>; }
struct TArgs_numeric_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_eq() { return &GenericExec<TPgDirectFunc<&numeric_eq>, true, true, TArgs_numeric_eq_Policy>; }
struct TArgs_numeric_exp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_exp() { return &GenericExec<TPgDirectFunc<&numeric_exp>, true, false, TArgs_numeric_exp_Policy>; }
struct TArgs_numeric_fac_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_fac() { return &GenericExec<TPgDirectFunc<&numeric_fac>, true, false, TArgs_numeric_fac_Policy>; }
struct TArgs_numeric_float4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_float4() { return &GenericExec<TPgDirectFunc<&numeric_float4>, true, true, TArgs_numeric_float4_Policy>; }
struct TArgs_numeric_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_float8() { return &GenericExec<TPgDirectFunc<&numeric_float8>, true, true, TArgs_numeric_float8_Policy>; }
struct TArgs_numeric_floor_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_floor() { return &GenericExec<TPgDirectFunc<&numeric_floor>, true, false, TArgs_numeric_floor_Policy>; }
struct TArgs_numeric_gcd_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_gcd() { return &GenericExec<TPgDirectFunc<&numeric_gcd>, true, false, TArgs_numeric_gcd_Policy>; }
struct TArgs_numeric_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_ge() { return &GenericExec<TPgDirectFunc<&numeric_ge>, true, true, TArgs_numeric_ge_Policy>; }
struct TArgs_numeric_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_gt() { return &GenericExec<TPgDirectFunc<&numeric_gt>, true, true, TArgs_numeric_gt_Policy>; }
struct TArgs_numeric_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_numeric_in() { return &GenericExec<TPgDirectFunc<&numeric_in>, true, false, TArgs_numeric_in_Policy>; }
struct TArgs_numeric_inc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_inc() { return &GenericExec<TPgDirectFunc<&numeric_inc>, true, false, TArgs_numeric_inc_Policy>; }
struct TArgs_numeric_int2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_int2() { return &GenericExec<TPgDirectFunc<&numeric_int2>, true, true, TArgs_numeric_int2_Policy>; }
struct TArgs_numeric_int4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_int4() { return &GenericExec<TPgDirectFunc<&numeric_int4>, true, true, TArgs_numeric_int4_Policy>; }
struct TArgs_numeric_int8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_int8() { return &GenericExec<TPgDirectFunc<&numeric_int8>, true, true, TArgs_numeric_int8_Policy>; }
struct TArgs_numeric_larger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_larger() { return &GenericExec<TPgDirectFunc<&numeric_larger>, true, false, TArgs_numeric_larger_Policy>; }
struct TArgs_numeric_lcm_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_lcm() { return &GenericExec<TPgDirectFunc<&numeric_lcm>, true, false, TArgs_numeric_lcm_Policy>; }
struct TArgs_numeric_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_le() { return &GenericExec<TPgDirectFunc<&numeric_le>, true, true, TArgs_numeric_le_Policy>; }
struct TArgs_numeric_ln_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_ln() { return &GenericExec<TPgDirectFunc<&numeric_ln>, true, false, TArgs_numeric_ln_Policy>; }
struct TArgs_numeric_log_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_log() { return &GenericExec<TPgDirectFunc<&numeric_log>, true, false, TArgs_numeric_log_Policy>; }
struct TArgs_numeric_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_lt() { return &GenericExec<TPgDirectFunc<&numeric_lt>, true, true, TArgs_numeric_lt_Policy>; }
struct TArgs_numeric_min_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_min_scale() { return &GenericExec<TPgDirectFunc<&numeric_min_scale>, true, true, TArgs_numeric_min_scale_Policy>; }
struct TArgs_numeric_mod_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_mod() { return &GenericExec<TPgDirectFunc<&numeric_mod>, true, false, TArgs_numeric_mod_Policy>; }
struct TArgs_numeric_mul_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_mul() { return &GenericExec<TPgDirectFunc<&numeric_mul>, true, false, TArgs_numeric_mul_Policy>; }
struct TArgs_numeric_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_ne() { return &GenericExec<TPgDirectFunc<&numeric_ne>, true, true, TArgs_numeric_ne_Policy>; }
struct TArgs_numeric_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_out() { return &GenericExec<TPgDirectFunc<&numeric_out>, true, false, TArgs_numeric_out_Policy>; }
struct TArgs_numeric_pg_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_pg_lsn() { return &GenericExec<TPgDirectFunc<&numeric_pg_lsn>, true, true, TArgs_numeric_pg_lsn_Policy>; }
struct TArgs_numeric_poly_avg_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_avg() { return &GenericExec<TPgDirectFunc<&numeric_poly_avg>, false, false, TArgs_numeric_poly_avg_Policy>; }
struct TArgs_numeric_poly_combine_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_numeric_poly_combine() { return &GenericExec<TPgDirectFunc<&numeric_poly_combine>, false, true, TArgs_numeric_poly_combine_Policy>; }
struct TArgs_numeric_poly_deserialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric_poly_deserialize() { return &GenericExec<TPgDirectFunc<&numeric_poly_deserialize>, true, true, TArgs_numeric_poly_deserialize_Policy>; }
struct TArgs_numeric_poly_serialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_serialize() { return &GenericExec<TPgDirectFunc<&numeric_poly_serialize>, true, false, TArgs_numeric_poly_serialize_Policy>; }
struct TArgs_numeric_poly_stddev_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_stddev_pop() { return &GenericExec<TPgDirectFunc<&numeric_poly_stddev_pop>, false, false, TArgs_numeric_poly_stddev_pop_Policy>; }
struct TArgs_numeric_poly_stddev_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_stddev_samp() { return &GenericExec<TPgDirectFunc<&numeric_poly_stddev_samp>, false, false, TArgs_numeric_poly_stddev_samp_Policy>; }
struct TArgs_numeric_poly_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_sum() { return &GenericExec<TPgDirectFunc<&numeric_poly_sum>, false, false, TArgs_numeric_poly_sum_Policy>; }
struct TArgs_numeric_poly_var_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_var_pop() { return &GenericExec<TPgDirectFunc<&numeric_poly_var_pop>, false, false, TArgs_numeric_poly_var_pop_Policy>; }
struct TArgs_numeric_poly_var_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_poly_var_samp() { return &GenericExec<TPgDirectFunc<&numeric_poly_var_samp>, false, false, TArgs_numeric_poly_var_samp_Policy>; }
struct TArgs_numeric_power_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_power() { return &GenericExec<TPgDirectFunc<&numeric_power>, true, false, TArgs_numeric_power_Policy>; }
struct TArgs_numeric_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_numeric_recv() { return &GenericExec<TPgDirectFunc<&numeric_recv>, true, false, TArgs_numeric_recv_Policy>; }
struct TArgs_numeric_round_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric_round() { return &GenericExec<TPgDirectFunc<&numeric_round>, true, false, TArgs_numeric_round_Policy>; }
struct TArgs_numeric_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_scale() { return &GenericExec<TPgDirectFunc<&numeric_scale>, true, true, TArgs_numeric_scale_Policy>; }
struct TArgs_numeric_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_send() { return &GenericExec<TPgDirectFunc<&numeric_send>, true, false, TArgs_numeric_send_Policy>; }
struct TArgs_numeric_serialize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_serialize() { return &GenericExec<TPgDirectFunc<&numeric_serialize>, true, false, TArgs_numeric_serialize_Policy>; }
struct TArgs_numeric_sign_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_sign() { return &GenericExec<TPgDirectFunc<&numeric_sign>, true, false, TArgs_numeric_sign_Policy>; }
struct TArgs_numeric_smaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_smaller() { return &GenericExec<TPgDirectFunc<&numeric_smaller>, true, false, TArgs_numeric_smaller_Policy>; }
struct TArgs_numeric_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_sortsupport() { return &GenericExec<TPgDirectFunc<&numeric_sortsupport>, true, true, TArgs_numeric_sortsupport_Policy>; }
struct TArgs_numeric_sqrt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_sqrt() { return &GenericExec<TPgDirectFunc<&numeric_sqrt>, true, false, TArgs_numeric_sqrt_Policy>; }
struct TArgs_numeric_stddev_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_stddev_pop() { return &GenericExec<TPgDirectFunc<&numeric_stddev_pop>, false, false, TArgs_numeric_stddev_pop_Policy>; }
struct TArgs_numeric_stddev_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_stddev_samp() { return &GenericExec<TPgDirectFunc<&numeric_stddev_samp>, false, false, TArgs_numeric_stddev_samp_Policy>; }
struct TArgs_numeric_sub_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_numeric_sub() { return &GenericExec<TPgDirectFunc<&numeric_sub>, true, false, TArgs_numeric_sub_Policy>; }
struct TArgs_numeric_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_sum() { return &GenericExec<TPgDirectFunc<&numeric_sum>, false, false, TArgs_numeric_sum_Policy>; }
struct TArgs_numeric_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_support() { return &GenericExec<TPgDirectFunc<&numeric_support>, true, true, TArgs_numeric_support_Policy>; }
struct TArgs_numeric_trim_scale_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_trim_scale() { return &GenericExec<TPgDirectFunc<&numeric_trim_scale>, true, false, TArgs_numeric_trim_scale_Policy>; }
struct TArgs_numeric_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_numeric_trunc() { return &GenericExec<TPgDirectFunc<&numeric_trunc>, true, false, TArgs_numeric_trunc_Policy>; }
struct TArgs_numeric_uminus_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_uminus() { return &GenericExec<TPgDirectFunc<&numeric_uminus>, true, false, TArgs_numeric_uminus_Policy>; }
struct TArgs_numeric_uplus_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numeric_uplus() { return &GenericExec<TPgDirectFunc<&numeric_uplus>, true, false, TArgs_numeric_uplus_Policy>; }
struct TArgs_numeric_var_pop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_var_pop() { return &GenericExec<TPgDirectFunc<&numeric_var_pop>, false, false, TArgs_numeric_var_pop_Policy>; }
struct TArgs_numeric_var_samp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numeric_var_samp() { return &GenericExec<TPgDirectFunc<&numeric_var_samp>, false, false, TArgs_numeric_var_samp_Policy>; }
struct TArgs_numerictypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_numerictypmodin() { return &GenericExec<TPgDirectFunc<&numerictypmodin>, true, true, TArgs_numerictypmodin_Policy>; }
struct TArgs_numerictypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_numerictypmodout() { return &GenericExec<TPgDirectFunc<&numerictypmodout>, true, false, TArgs_numerictypmodout_Policy>; }
struct TArgs_pg_client_encoding_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_client_encoding() { return &GenericExec<TPgDirectFunc<&pg_client_encoding>, true, false, TArgs_pg_client_encoding_Policy>; }
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
struct TArgs_pg_ddl_command_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_ddl_command_in() { return &GenericExec<TPgDirectFunc<&pg_ddl_command_in>, true, true, TArgs_pg_ddl_command_in_Policy>; }
struct TArgs_pg_ddl_command_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_out() { return &GenericExec<TPgDirectFunc<&pg_ddl_command_out>, true, false, TArgs_pg_ddl_command_out_Policy>; }
struct TArgs_pg_ddl_command_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_recv() { return &GenericExec<TPgDirectFunc<&pg_ddl_command_recv>, true, true, TArgs_pg_ddl_command_recv_Policy>; }
struct TArgs_pg_ddl_command_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_ddl_command_send() { return &GenericExec<TPgDirectFunc<&pg_ddl_command_send>, true, false, TArgs_pg_ddl_command_send_Policy>; }
struct TArgs_pg_describe_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_describe_object() { return &GenericExec<TPgDirectFunc<&pg_describe_object>, true, false, TArgs_pg_describe_object_Policy>; }
struct TArgs_pg_encoding_max_length_sql_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_encoding_max_length_sql() { return &GenericExec<TPgDirectFunc<&pg_encoding_max_length_sql>, true, true, TArgs_pg_encoding_max_length_sql_Policy>; }
struct TArgs_pg_extension_config_dump_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_extension_config_dump() { return &GenericExec<TPgDirectFunc<&pg_extension_config_dump>, true, true, TArgs_pg_extension_config_dump_Policy>; }
struct TArgs_pg_get_object_address_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_pg_get_object_address() { return &GenericExec<TPgDirectFunc<&pg_get_object_address>, true, false, TArgs_pg_get_object_address_Policy>; }
struct TArgs_pg_identify_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_identify_object() { return &GenericExec<TPgDirectFunc<&pg_identify_object>, true, false, TArgs_pg_identify_object_Policy>; }
struct TArgs_pg_identify_object_as_address_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_identify_object_as_address() { return &GenericExec<TPgDirectFunc<&pg_identify_object_as_address>, true, false, TArgs_pg_identify_object_as_address_Policy>; }
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
struct TArgs_pg_jit_available_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_jit_available() { return &GenericExec<TPgDirectFunc<&pg_jit_available>, true, true, TArgs_pg_jit_available_Policy>; }
struct TArgs_pg_nextoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
TExecFunc arrow_pg_nextoid() { return &GenericExec<TPgDirectFunc<&pg_nextoid>, true, true, TArgs_pg_nextoid_Policy>; }
struct TArgs_pg_node_tree_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_in() { return &GenericExec<TPgDirectFunc<&pg_node_tree_in>, true, false, TArgs_pg_node_tree_in_Policy>; }
struct TArgs_pg_node_tree_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_out() { return &GenericExec<TPgDirectFunc<&pg_node_tree_out>, true, false, TArgs_pg_node_tree_out_Policy>; }
struct TArgs_pg_node_tree_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_node_tree_recv() { return &GenericExec<TPgDirectFunc<&pg_node_tree_recv>, true, false, TArgs_pg_node_tree_recv_Policy>; }
struct TArgs_pg_node_tree_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_node_tree_send() { return &GenericExec<TPgDirectFunc<&pg_node_tree_send>, true, false, TArgs_pg_node_tree_send_Policy>; }
struct TArgs_pg_relation_is_publishable_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_relation_is_publishable() { return &GenericExec<TPgDirectFunc<&pg_relation_is_publishable>, true, true, TArgs_pg_relation_is_publishable_Policy>; }
struct TArgs_phraseto_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_phraseto_tsquery() { return &GenericExec<TPgDirectFunc<&phraseto_tsquery>, true, false, TArgs_phraseto_tsquery_Policy>; }
struct TArgs_phraseto_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_phraseto_tsquery_byid() { return &GenericExec<TPgDirectFunc<&phraseto_tsquery_byid>, true, false, TArgs_phraseto_tsquery_byid_Policy>; }
struct TArgs_plainto_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_plainto_tsquery() { return &GenericExec<TPgDirectFunc<&plainto_tsquery>, true, false, TArgs_plainto_tsquery_Policy>; }
struct TArgs_plainto_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_plainto_tsquery_byid() { return &GenericExec<TPgDirectFunc<&plainto_tsquery_byid>, true, false, TArgs_plainto_tsquery_byid_Policy>; }
struct TArgs_postgresql_fdw_validator_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_postgresql_fdw_validator() { return &GenericExec<TPgDirectFunc<&postgresql_fdw_validator>, true, true, TArgs_postgresql_fdw_validator_Policy>; }
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
struct TArgs_repeat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_repeat() { return &GenericExec<TPgDirectFunc<&repeat>, true, false, TArgs_repeat_Policy>; }
struct TArgs_replace_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_replace_text() { return &GenericExec<TPgDirectFunc<&replace_text>, true, false, TArgs_replace_text_Policy>; }
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
struct TArgs_set_config_by_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
TExecFunc arrow_set_config_by_name() { return &GenericExec<TPgDirectFunc<&set_config_by_name>, false, false, TArgs_set_config_by_name_Policy>; }
struct TArgs_shell_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_shell_in() { return &GenericExec<TPgDirectFunc<&shell_in>, false, true, TArgs_shell_in_Policy>; }
struct TArgs_shell_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_shell_out() { return &GenericExec<TPgDirectFunc<&shell_out>, true, false, TArgs_shell_out_Policy>; }
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
struct TArgs_suppress_redundant_updates_trigger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_suppress_redundant_updates_trigger() { return &GenericExec<TPgDirectFunc<&suppress_redundant_updates_trigger>, true, true, TArgs_suppress_redundant_updates_trigger_Policy>; }
struct TArgs_table_am_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_table_am_handler_in() { return &GenericExec<TPgDirectFunc<&table_am_handler_in>, false, true, TArgs_table_am_handler_in_Policy>; }
struct TArgs_table_am_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_table_am_handler_out() { return &GenericExec<TPgDirectFunc<&table_am_handler_out>, true, false, TArgs_table_am_handler_out_Policy>; }
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
struct TArgs_textsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_textsend() { return &GenericExec<TPgDirectFunc<&textsend>, true, false, TArgs_textsend_Policy>; }
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
struct TArgs_to_json_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_to_json() { return &GenericExec<TPgDirectFunc<&to_json>, true, false, TArgs_to_json_Policy>; }
struct TArgs_to_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_tsquery() { return &GenericExec<TPgDirectFunc<&to_tsquery>, true, false, TArgs_to_tsquery_Policy>; }
struct TArgs_to_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_to_tsquery_byid() { return &GenericExec<TPgDirectFunc<&to_tsquery_byid>, true, false, TArgs_to_tsquery_byid_Policy>; }
struct TArgs_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_to_tsvector() { return &GenericExec<TPgDirectFunc<&to_tsvector>, true, false, TArgs_to_tsvector_Policy>; }
struct TArgs_to_tsvector_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_to_tsvector_byid() { return &GenericExec<TPgDirectFunc<&to_tsvector_byid>, true, false, TArgs_to_tsvector_byid_Policy>; }
struct TArgs_translate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
TExecFunc arrow_translate() { return &GenericExec<TPgDirectFunc<&translate>, true, false, TArgs_translate_Policy>; }
struct TArgs_trigger_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_trigger_in() { return &GenericExec<TPgDirectFunc<&trigger_in>, false, true, TArgs_trigger_in_Policy>; }
struct TArgs_trigger_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_trigger_out() { return &GenericExec<TPgDirectFunc<&trigger_out>, true, false, TArgs_trigger_out_Policy>; }
struct TArgs_trim_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_trim_array() { return &GenericExec<TPgDirectFunc<&trim_array>, true, false, TArgs_trim_array_Policy>; }
struct TArgs_tsm_handler_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsm_handler_in() { return &GenericExec<TPgDirectFunc<&tsm_handler_in>, false, true, TArgs_tsm_handler_in_Policy>; }
struct TArgs_tsm_handler_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_handler_out() { return &GenericExec<TPgDirectFunc<&tsm_handler_out>, true, false, TArgs_tsm_handler_out_Policy>; }
struct TArgs_tsm_system_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsm_system_handler() { return &GenericExec<TPgDirectFunc<&tsm_system_handler>, true, true, TArgs_tsm_system_handler_Policy>; }
struct TArgs_tsqueryin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsqueryin() { return &GenericExec<TPgDirectFunc<&tsqueryin>, true, false, TArgs_tsqueryin_Policy>; }
struct TArgs_tsqueryout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsqueryout() { return &GenericExec<TPgDirectFunc<&tsqueryout>, true, false, TArgs_tsqueryout_Policy>; }
struct TArgs_tsqueryrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_tsqueryrecv() { return &GenericExec<TPgDirectFunc<&tsqueryrecv>, true, false, TArgs_tsqueryrecv_Policy>; }
struct TArgs_tsquerysend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquerysend() { return &GenericExec<TPgDirectFunc<&tsquerysend>, true, false, TArgs_tsquerysend_Policy>; }
struct TArgs_tsquerytree_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_tsquerytree() { return &GenericExec<TPgDirectFunc<&tsquerytree>, true, false, TArgs_tsquerytree_Policy>; }
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
struct TArgs_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_upper() { return &GenericExec<TPgDirectFunc<&upper>, true, false, TArgs_upper_Policy>; }
struct TArgs_varbit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varbit() { return &GenericExec<TPgDirectFunc<&varbit>, true, false, TArgs_varbit_Policy>; }
struct TArgs_varbit_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
TExecFunc arrow_varbit_in() { return &GenericExec<TPgDirectFunc<&varbit_in>, true, false, TArgs_varbit_in_Policy>; }
struct TArgs_varbit_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varbit_out() { return &GenericExec<TPgDirectFunc<&varbit_out>, true, false, TArgs_varbit_out_Policy>; }
struct TArgs_varbit_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_varbit_recv() { return &GenericExec<TPgDirectFunc<&varbit_recv>, true, false, TArgs_varbit_recv_Policy>; }
struct TArgs_varbit_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varbit_send() { return &GenericExec<TPgDirectFunc<&varbit_send>, true, false, TArgs_varbit_send_Policy>; }
struct TArgs_varbit_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varbit_support() { return &GenericExec<TPgDirectFunc<&varbit_support>, true, true, TArgs_varbit_support_Policy>; }
struct TArgs_varbittypmodin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_varbittypmodin() { return &GenericExec<TPgDirectFunc<&varbittypmodin>, true, true, TArgs_varbittypmodin_Policy>; }
struct TArgs_varbittypmodout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_varbittypmodout() { return &GenericExec<TPgDirectFunc<&varbittypmodout>, true, false, TArgs_varbittypmodout_Policy>; }
struct TArgs_void_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_void_in() { return &GenericExec<TPgDirectFunc<&void_in>, true, true, TArgs_void_in_Policy>; }
struct TArgs_void_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_out() { return &GenericExec<TPgDirectFunc<&void_out>, true, false, TArgs_void_out_Policy>; }
struct TArgs_void_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_recv() { return &GenericExec<TPgDirectFunc<&void_recv>, true, true, TArgs_void_recv_Policy>; }
struct TArgs_void_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_void_send() { return &GenericExec<TPgDirectFunc<&void_send>, true, false, TArgs_void_send_Policy>; }
struct TArgs_websearch_to_tsquery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_websearch_to_tsquery() { return &GenericExec<TPgDirectFunc<&websearch_to_tsquery>, true, false, TArgs_websearch_to_tsquery_Policy>; }
struct TArgs_websearch_to_tsquery_byid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_websearch_to_tsquery_byid() { return &GenericExec<TPgDirectFunc<&websearch_to_tsquery_byid>, true, false, TArgs_websearch_to_tsquery_byid_Policy>; }
struct TArgs_width_bucket_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_width_bucket_array() { return &GenericExec<TPgDirectFunc<&width_bucket_array>, true, true, TArgs_width_bucket_array_Policy>; }
struct TArgs_width_bucket_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_width_bucket_numeric() { return &GenericExec<TPgDirectFunc<&width_bucket_numeric>, true, true, TArgs_width_bucket_numeric_Policy>; }
