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
struct TArgs_array_cat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_array_cat() { return &GenericExec<TPgDirectFunc<&array_cat>, false, false, TArgs_array_cat_Policy>; }
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
struct TArgs_array_subscript_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_subscript_handler() { return &GenericExec<TPgDirectFunc<&array_subscript_handler>, true, true, TArgs_array_subscript_handler_Policy>; }
struct TArgs_array_to_tsvector_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_array_to_tsvector() { return &GenericExec<TPgDirectFunc<&array_to_tsvector>, true, false, TArgs_array_to_tsvector_Policy>; }
struct TArgs_array_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_array_typanalyze() { return &GenericExec<TPgDirectFunc<&array_typanalyze>, true, true, TArgs_array_typanalyze_Policy>; }
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
struct TArgs_currval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_currval_oid() { return &GenericExec<TPgDirectFunc<&currval_oid>, true, true, TArgs_currval_oid_Policy>; }
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
struct TArgs_float4_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_float4_to_char() { return &GenericExec<TPgDirectFunc<&float4_to_char>, true, false, TArgs_float4_to_char_Policy>; }
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
struct TArgs_float8_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_float8_to_char() { return &GenericExec<TPgDirectFunc<&float8_to_char>, true, false, TArgs_float8_to_char_Policy>; }
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
struct TArgs_interval_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_interval_to_char() { return &GenericExec<TPgDirectFunc<&interval_to_char>, true, false, TArgs_interval_to_char_Policy>; }
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
struct TArgs_makeaclitem_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,false,true};
};
TExecFunc arrow_makeaclitem() { return &GenericExec<TPgDirectFunc<&makeaclitem>, true, false, TArgs_makeaclitem_Policy>; }
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
struct TArgs_nextval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_nextval_oid() { return &GenericExec<TPgDirectFunc<&nextval_oid>, true, true, TArgs_nextval_oid_Policy>; }
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
struct TArgs_pg_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_blocking_pids() { return &GenericExec<TPgDirectFunc<&pg_blocking_pids>, true, false, TArgs_pg_blocking_pids_Policy>; }
struct TArgs_pg_cancel_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_cancel_backend() { return &GenericExec<TPgDirectFunc<&pg_cancel_backend>, true, true, TArgs_pg_cancel_backend_Policy>; }
struct TArgs_pg_collation_actual_version_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_collation_actual_version() { return &GenericExec<TPgDirectFunc<&pg_collation_actual_version>, true, false, TArgs_pg_collation_actual_version_Policy>; }
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
struct TArgs_pg_drop_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
TExecFunc arrow_pg_drop_replication_slot() { return &GenericExec<TPgDirectFunc<&pg_drop_replication_slot>, true, true, TArgs_pg_drop_replication_slot_Policy>; }
struct TArgs_pg_filenode_relation_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_filenode_relation() { return &GenericExec<TPgDirectFunc<&pg_filenode_relation>, true, true, TArgs_pg_filenode_relation_Policy>; }
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
struct TArgs_pg_isolation_test_session_is_blocked_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_isolation_test_session_is_blocked() { return &GenericExec<TPgDirectFunc<&pg_isolation_test_session_is_blocked>, true, true, TArgs_pg_isolation_test_session_is_blocked_Policy>; }
struct TArgs_pg_last_committed_xact_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_last_committed_xact() { return &GenericExec<TPgDirectFunc<&pg_last_committed_xact>, true, false, TArgs_pg_last_committed_xact_Policy>; }
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
struct TArgs_pg_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_relation_size() { return &GenericExec<TPgDirectFunc<&pg_relation_size>, true, true, TArgs_pg_relation_size_Policy>; }
struct TArgs_pg_reload_conf_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_reload_conf() { return &GenericExec<TPgDirectFunc<&pg_reload_conf>, true, true, TArgs_pg_reload_conf_Policy>; }
struct TArgs_pg_replication_slot_advance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
TExecFunc arrow_pg_replication_slot_advance() { return &GenericExec<TPgDirectFunc<&pg_replication_slot_advance>, true, false, TArgs_pg_replication_slot_advance_Policy>; }
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
struct TArgs_pg_safe_snapshot_blocking_pids_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_safe_snapshot_blocking_pids() { return &GenericExec<TPgDirectFunc<&pg_safe_snapshot_blocking_pids>, true, false, TArgs_pg_safe_snapshot_blocking_pids_Policy>; }
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
struct TArgs_pg_stat_get_wal_receiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_stat_get_wal_receiver() { return &GenericExec<TPgDirectFunc<&pg_stat_get_wal_receiver>, false, false, TArgs_pg_stat_get_wal_receiver_Policy>; }
struct TArgs_pg_table_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_table_size() { return &GenericExec<TPgDirectFunc<&pg_table_size>, true, true, TArgs_pg_table_size_Policy>; }
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
struct TArgs_pg_terminate_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
TExecFunc arrow_pg_terminate_backend() { return &GenericExec<TPgDirectFunc<&pg_terminate_backend>, true, true, TArgs_pg_terminate_backend_Policy>; }
struct TArgs_pg_total_relation_size_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_total_relation_size() { return &GenericExec<TPgDirectFunc<&pg_total_relation_size>, true, true, TArgs_pg_total_relation_size_Policy>; }
struct TArgs_pg_trigger_depth_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pg_trigger_depth() { return &GenericExec<TPgDirectFunc<&pg_trigger_depth>, true, true, TArgs_pg_trigger_depth_Policy>; }
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
struct TArgs_pg_visible_in_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_pg_visible_in_snapshot() { return &GenericExec<TPgDirectFunc<&pg_visible_in_snapshot>, true, true, TArgs_pg_visible_in_snapshot_Policy>; }
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
struct TArgs_pg_xact_status_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_pg_xact_status() { return &GenericExec<TPgDirectFunc<&pg_xact_status>, true, false, TArgs_pg_xact_status_Policy>; }
struct TArgs_pgsql_version_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_pgsql_version() { return &GenericExec<TPgDirectFunc<&pgsql_version>, true, false, TArgs_pgsql_version_Policy>; }
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
struct TArgs_radians_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_radians() { return &GenericExec<TPgDirectFunc<&radians>, true, true, TArgs_radians_Policy>; }
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
struct TArgs_setseed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
TExecFunc arrow_setseed() { return &GenericExec<TPgDirectFunc<&setseed>, true, true, TArgs_setseed_Policy>; }
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
struct TArgs_to_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
TExecFunc arrow_to_timestamp() { return &GenericExec<TPgDirectFunc<&to_timestamp>, true, true, TArgs_to_timestamp_Policy>; }
struct TArgs_ts_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
TExecFunc arrow_ts_lexize() { return &GenericExec<TPgDirectFunc<&ts_lexize>, true, false, TArgs_ts_lexize_Policy>; }
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
struct TArgs_unique_key_recheck_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
TExecFunc arrow_unique_key_recheck() { return &GenericExec<TPgDirectFunc<&unique_key_recheck>, true, true, TArgs_unique_key_recheck_Policy>; }
struct TArgs_width_bucket_float8_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
TExecFunc arrow_width_bucket_float8() { return &GenericExec<TPgDirectFunc<&width_bucket_float8>, true, true, TArgs_width_bucket_float8_Policy>; }
