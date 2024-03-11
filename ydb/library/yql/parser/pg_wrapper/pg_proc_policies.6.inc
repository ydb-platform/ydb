struct TArgs_currval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_domain_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
struct TArgs_domain_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_hashhandler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_lastval_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_nextval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_backup_start_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_collation_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_control_checkpoint_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_control_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_control_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_control_system_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_conversion_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_create_restore_point_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_current_wal_flush_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_current_wal_insert_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_current_wal_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_function_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_get_wal_replay_pause_state_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_is_in_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_is_in_recovery_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_is_other_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_is_wal_replay_paused_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_last_wal_receive_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_last_wal_replay_lsn_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_last_xact_replay_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_my_temp_schema_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_opclass_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_operator_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_opfamily_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_promote_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_pg_sequence_last_value_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_sequence_parameters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_start_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
struct TArgs_pg_stat_clear_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_analyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_archiver_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_autoanalyze_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_autovacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_activity_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_activity_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_client_addr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_client_port_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_dbid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_pid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_userid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_wait_event_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_wait_event_type_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_backend_xact_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_bgwriter_buf_written_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_bgwriter_buf_written_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_bgwriter_maxwritten_clean_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_bgwriter_requested_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_bgwriter_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_bgwriter_timed_checkpoints_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_buf_alloc_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_buf_fsync_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_buf_written_backend_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_checkpoint_sync_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_checkpoint_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_db_active_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_blk_read_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_blk_write_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_checksum_failures_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_checksum_last_failure_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_all_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_bufferpin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_lock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_snapshot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_startup_deadlock_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_conflict_tablespace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_deadlocks_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_idle_in_transaction_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_numbackends_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_session_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_sessions_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_sessions_abandoned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_sessions_fatal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_sessions_killed_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_stat_reset_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_temp_bytes_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_temp_files_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_xact_commit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_db_xact_rollback_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_dead_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_ins_since_vacuum_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_last_analyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_last_autoanalyze_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_last_autovacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_last_vacuum_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_live_tuples_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_mod_since_analyze_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_snapshot_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_vacuum_count_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_get_xact_blocks_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_blocks_hit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_function_calls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_function_self_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_function_total_time_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_numscans_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_deleted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_fetched_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_hot_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_inserted_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_returned_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_get_xact_tuples_updated_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_stat_reset_replication_slot_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_stat_reset_shared_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_stat_reset_single_function_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_reset_single_table_counters_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stat_reset_slru_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_statistics_obj_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_stop_backup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_switch_wal_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_table_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_trigger_depth_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_ts_config_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_ts_dict_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_ts_parser_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_ts_template_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_type_is_visible_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_wal_lsn_diff_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_pg_wal_replay_pause_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_wal_replay_resume_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_walfile_name_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_walfile_name_offset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_prsd_end_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_prsd_headline_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
struct TArgs_prsd_lextype_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_prsd_nexttoken_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_prsd_start_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_setval3_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_setval_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_tsm_system_handler_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
