struct TArgs_array_agg_array_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_array_agg_array_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_array_agg_finalfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_array_agg_transfn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_array_append_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_array_cat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_array_position_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_array_position_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
struct TArgs_array_positions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_array_prepend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_array_typanalyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_bttidcmp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_currtid_byrelname_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_hashtid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_hashtidextended_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_pg_current_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_current_xact_id_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_current_xact_id_if_assigned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_describe_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_pg_get_object_address_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_pg_identify_object_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_pg_identify_object_as_address_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_pg_snapshot_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_snapshot_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_snapshot_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_snapshot_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_snapshot_xmax_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_snapshot_xmin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_visible_in_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_pg_xact_status_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_row_security_active_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_row_security_active_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_tideq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidge_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidgt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_tidlarger_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidle_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidlt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tidout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_tidrecv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_tidsend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_tidsmaller_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_ts_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
