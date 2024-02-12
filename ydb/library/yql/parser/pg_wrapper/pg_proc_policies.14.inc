struct TArgs_btrecordcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_btrecordimagecmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_cideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_cidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_cidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_cidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_cidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_hash_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_hash_record_extended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_hashmacaddr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_hashmacaddrextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_macaddr_and_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_macaddr_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_not_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_macaddr_or_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_macaddr_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_macaddr_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_macaddr_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_macaddr_sortsupport_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_macaddr_trunc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_mxid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_index_column_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
struct TArgs_pg_index_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_pg_indexam_has_property_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_pg_indexam_progress_phasename_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_rangesel_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
struct TArgs_record_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_image_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
struct TArgs_record_le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_record_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_record_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_record_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_spg_text_choose_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_spg_text_config_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_spg_text_inner_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_spg_text_leaf_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_spg_text_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_suppress_redundant_updates_trigger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_xid8cmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8eq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8ge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8gt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_xid8le_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8lt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8ne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xid8out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xid8recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xid8send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xid8toxid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xid_age_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_xidneq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_xidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_xidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
