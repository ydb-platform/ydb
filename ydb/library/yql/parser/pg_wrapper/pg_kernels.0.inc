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
struct TArgs_be_lo_close_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_close() { return &GenericExec<TPgDirectFunc<&be_lo_close>, true, true, TArgs_be_lo_close_Policy>; }
struct TArgs_be_lo_creat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_creat() { return &GenericExec<TPgDirectFunc<&be_lo_creat>, true, true, TArgs_be_lo_creat_Policy>; }
struct TArgs_be_lo_create_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_create() { return &GenericExec<TPgDirectFunc<&be_lo_create>, true, true, TArgs_be_lo_create_Policy>; }
struct TArgs_be_lo_export_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_be_lo_export() { return &GenericExec<TPgDirectFunc<&be_lo_export>, true, true, TArgs_be_lo_export_Policy>; }
struct TArgs_be_lo_from_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_be_lo_from_bytea() { return &GenericExec<TPgDirectFunc<&be_lo_from_bytea>, true, true, TArgs_be_lo_from_bytea_Policy>; }
struct TArgs_be_lo_get_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_get() { return &GenericExec<TPgDirectFunc<&be_lo_get>, true, false, TArgs_be_lo_get_Policy>; }
struct TArgs_be_lo_get_fragment_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_be_lo_get_fragment() { return &GenericExec<TPgDirectFunc<&be_lo_get_fragment>, true, false, TArgs_be_lo_get_fragment_Policy>; }
struct TArgs_be_lo_import_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_be_lo_import() { return &GenericExec<TPgDirectFunc<&be_lo_import>, true, true, TArgs_be_lo_import_Policy>; }
struct TArgs_be_lo_import_with_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_be_lo_import_with_oid() { return &GenericExec<TPgDirectFunc<&be_lo_import_with_oid>, true, true, TArgs_be_lo_import_with_oid_Policy>; }
struct TArgs_be_lo_lseek_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_be_lo_lseek() { return &GenericExec<TPgDirectFunc<&be_lo_lseek>, true, true, TArgs_be_lo_lseek_Policy>; }
struct TArgs_be_lo_lseek64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_be_lo_lseek64() { return &GenericExec<TPgDirectFunc<&be_lo_lseek64>, true, true, TArgs_be_lo_lseek64_Policy>; }
struct TArgs_be_lo_open_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_be_lo_open() { return &GenericExec<TPgDirectFunc<&be_lo_open>, true, true, TArgs_be_lo_open_Policy>; }
struct TArgs_be_lo_put_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
TExecFunc arrow_be_lo_put() { return &GenericExec<TPgDirectFunc<&be_lo_put>, true, true, TArgs_be_lo_put_Policy>; }
struct TArgs_be_lo_tell_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_tell() { return &GenericExec<TPgDirectFunc<&be_lo_tell>, true, true, TArgs_be_lo_tell_Policy>; }
struct TArgs_be_lo_tell64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_tell64() { return &GenericExec<TPgDirectFunc<&be_lo_tell64>, true, true, TArgs_be_lo_tell64_Policy>; }
struct TArgs_be_lo_truncate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_be_lo_truncate() { return &GenericExec<TPgDirectFunc<&be_lo_truncate>, true, true, TArgs_be_lo_truncate_Policy>; }
struct TArgs_be_lo_truncate64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_be_lo_truncate64() { return &GenericExec<TPgDirectFunc<&be_lo_truncate64>, true, true, TArgs_be_lo_truncate64_Policy>; }
struct TArgs_be_lo_unlink_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_be_lo_unlink() { return &GenericExec<TPgDirectFunc<&be_lo_unlink>, true, true, TArgs_be_lo_unlink_Policy>; }
struct TArgs_be_loread_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_be_loread() { return &GenericExec<TPgDirectFunc<&be_loread>, true, false, TArgs_be_loread_Policy>; }
struct TArgs_be_lowrite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_be_lowrite() { return &GenericExec<TPgDirectFunc<&be_lowrite>, true, true, TArgs_be_lowrite_Policy>; }
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
struct TArgs_clock_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_clock_timestamp() { return &GenericExec<TPgDirectFunc<&clock_timestamp>, true, true, TArgs_clock_timestamp_Policy>; }
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
struct TArgs_dsimple_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_dsimple_init() { return &GenericExec<TPgDirectFunc<&dsimple_init>, true, true, TArgs_dsimple_init_Policy>; }
struct TArgs_dsimple_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_dsimple_lexize() { return &GenericExec<TPgDirectFunc<&dsimple_lexize>, true, true, TArgs_dsimple_lexize_Policy>; }
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
struct TArgs_float8_timestamptz_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_float8_timestamptz() { return &GenericExec<TPgDirectFunc<&float8_timestamptz>, true, true, TArgs_float8_timestamptz_Policy>; }
struct TArgs_generate_series_int4_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_generate_series_int4_support() { return &GenericExec<TPgDirectFunc<&generate_series_int4_support>, true, true, TArgs_generate_series_int4_support_Policy>; }
struct TArgs_get_current_ts_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_get_current_ts_config() { return &GenericExec<TPgDirectFunc<&get_current_ts_config>, true, true, TArgs_get_current_ts_config_Policy>; }
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
struct TArgs_i2toi4_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i2toi4() { return &GenericExec<TPgDirectFunc<&i2toi4>, true, true, TArgs_i2toi4_Policy>; }
struct TArgs_i4toi2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_i4toi2() { return &GenericExec<TPgDirectFunc<&i4toi2>, true, true, TArgs_i4toi2_Policy>; }
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
struct TArgs_in_range_interval_interval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_interval_interval() { return &GenericExec<TPgDirectFunc<&in_range_interval_interval>, true, true, TArgs_in_range_interval_interval_Policy>; }
struct TArgs_in_range_numeric_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,true};
};
TExecFunc arrow_in_range_numeric_numeric() { return &GenericExec<TPgDirectFunc<&in_range_numeric_numeric>, true, true, TArgs_in_range_numeric_numeric_Policy>; }
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
struct TArgs_int2int4_sum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_int2int4_sum() { return &GenericExec<TPgDirectFunc<&int2int4_sum>, true, true, TArgs_int2int4_sum_Policy>; }
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
struct TArgs_int4_bool_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_int4_bool() { return &GenericExec<TPgDirectFunc<&int4_bool>, true, true, TArgs_int4_bool_Policy>; }
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
struct TArgs_multirangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_multirangesel() { return &GenericExec<TPgDirectFunc<&multirangesel>, true, true, TArgs_multirangesel_Policy>; }
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
struct TArgs_pg_cancel_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_cancel_backend() { return &GenericExec<TPgDirectFunc<&pg_cancel_backend>, true, true, TArgs_pg_cancel_backend_Policy>; }
struct TArgs_pg_conf_load_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_conf_load_time() { return &GenericExec<TPgDirectFunc<&pg_conf_load_time>, true, true, TArgs_pg_conf_load_time_Policy>; }
struct TArgs_pg_current_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_snapshot() { return &GenericExec<TPgDirectFunc<&pg_current_snapshot>, true, false, TArgs_pg_current_snapshot_Policy>; }
struct TArgs_pg_current_xact_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_xact_id() { return &GenericExec<TPgDirectFunc<&pg_current_xact_id>, true, true, TArgs_pg_current_xact_id_Policy>; }
struct TArgs_pg_current_xact_id_if_assigned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_current_xact_id_if_assigned() { return &GenericExec<TPgDirectFunc<&pg_current_xact_id_if_assigned>, true, true, TArgs_pg_current_xact_id_if_assigned_Policy>; }
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
struct TArgs_pg_describe_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
TExecFunc arrow_pg_describe_object() { return &GenericExec<TPgDirectFunc<&pg_describe_object>, true, false, TArgs_pg_describe_object_Policy>; }
struct TArgs_pg_export_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_export_snapshot() { return &GenericExec<TPgDirectFunc<&pg_export_snapshot>, true, false, TArgs_pg_export_snapshot_Policy>; }
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
struct TArgs_pg_logical_emit_message_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_logical_emit_message_bytea() { return &GenericExec<TPgDirectFunc<&pg_logical_emit_message_bytea>, true, true, TArgs_pg_logical_emit_message_bytea_Policy>; }
struct TArgs_pg_logical_emit_message_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,false};
};
TExecFunc arrow_pg_logical_emit_message_text() { return &GenericExec<TPgDirectFunc<&pg_logical_emit_message_text>, true, true, TArgs_pg_logical_emit_message_text_Policy>; }
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
struct TArgs_pg_postmaster_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_postmaster_start_time() { return &GenericExec<TPgDirectFunc<&pg_postmaster_start_time>, true, true, TArgs_pg_postmaster_start_time_Policy>; }
struct TArgs_pg_reload_conf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_reload_conf() { return &GenericExec<TPgDirectFunc<&pg_reload_conf>, true, true, TArgs_pg_reload_conf_Policy>; }
struct TArgs_pg_rotate_logfile_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_rotate_logfile() { return &GenericExec<TPgDirectFunc<&pg_rotate_logfile>, true, true, TArgs_pg_rotate_logfile_Policy>; }
struct TArgs_pg_rotate_logfile_v2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_rotate_logfile_v2() { return &GenericExec<TPgDirectFunc<&pg_rotate_logfile_v2>, true, true, TArgs_pg_rotate_logfile_v2_Policy>; }
struct TArgs_pg_snapshot_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_snapshot_in() { return &GenericExec<TPgDirectFunc<&pg_snapshot_in>, true, false, TArgs_pg_snapshot_in_Policy>; }
struct TArgs_pg_snapshot_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_snapshot_out() { return &GenericExec<TPgDirectFunc<&pg_snapshot_out>, true, false, TArgs_pg_snapshot_out_Policy>; }
struct TArgs_pg_snapshot_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_snapshot_recv() { return &GenericExec<TPgDirectFunc<&pg_snapshot_recv>, true, false, TArgs_pg_snapshot_recv_Policy>; }
struct TArgs_pg_snapshot_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_snapshot_send() { return &GenericExec<TPgDirectFunc<&pg_snapshot_send>, true, false, TArgs_pg_snapshot_send_Policy>; }
struct TArgs_pg_snapshot_xmax_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_snapshot_xmax() { return &GenericExec<TPgDirectFunc<&pg_snapshot_xmax>, true, true, TArgs_pg_snapshot_xmax_Policy>; }
struct TArgs_pg_snapshot_xmin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_snapshot_xmin() { return &GenericExec<TPgDirectFunc<&pg_snapshot_xmin>, true, true, TArgs_pg_snapshot_xmin_Policy>; }
struct TArgs_pg_stat_get_wal_receiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_wal_receiver() { return &GenericExec<TPgDirectFunc<&pg_stat_get_wal_receiver>, false, false, TArgs_pg_stat_get_wal_receiver_Policy>; }
struct TArgs_pg_terminate_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_terminate_backend() { return &GenericExec<TPgDirectFunc<&pg_terminate_backend>, true, true, TArgs_pg_terminate_backend_Policy>; }
struct TArgs_pg_visible_in_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_visible_in_snapshot() { return &GenericExec<TPgDirectFunc<&pg_visible_in_snapshot>, true, true, TArgs_pg_visible_in_snapshot_Policy>; }
struct TArgs_pg_xact_status_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_xact_status() { return &GenericExec<TPgDirectFunc<&pg_xact_status>, true, false, TArgs_pg_xact_status_Policy>; }
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
struct TArgs_texticlike_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_texticlike_support() { return &GenericExec<TPgDirectFunc<&texticlike_support>, true, true, TArgs_texticlike_support_Policy>; }
struct TArgs_texticregexeq_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_texticregexeq_support() { return &GenericExec<TPgDirectFunc<&texticregexeq_support>, true, true, TArgs_texticregexeq_support_Policy>; }
struct TArgs_textlike_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textlike_support() { return &GenericExec<TPgDirectFunc<&textlike_support>, true, true, TArgs_textlike_support_Policy>; }
struct TArgs_textregexeq_support_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_textregexeq_support() { return &GenericExec<TPgDirectFunc<&textregexeq_support>, true, true, TArgs_textregexeq_support_Policy>; }
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
struct TArgs_ts_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_ts_lexize() { return &GenericExec<TPgDirectFunc<&ts_lexize>, true, false, TArgs_ts_lexize_Policy>; }
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
struct TArgs_width_bucket_numeric_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
TExecFunc arrow_width_bucket_numeric() { return &GenericExec<TPgDirectFunc<&width_bucket_numeric>, true, true, TArgs_width_bucket_numeric_Policy>; }
