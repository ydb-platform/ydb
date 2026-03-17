/* DO NOT EDIT: automatically built by dist/s_include. */
#ifndef	_repmgr_ext_h_
#define	_repmgr_ext_h_

#if defined(__cplusplus)
extern "C" {
#endif

int __repmgr_init_recover __P((ENV *, DB_DISTAB *));
void __repmgr_handshake_marshal __P((ENV *, __repmgr_handshake_args *, u_int8_t *));
int __repmgr_handshake_unmarshal __P((ENV *, __repmgr_handshake_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_v3handshake_marshal __P((ENV *, __repmgr_v3handshake_args *, u_int8_t *));
int __repmgr_v3handshake_unmarshal __P((ENV *, __repmgr_v3handshake_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_v2handshake_marshal __P((ENV *, __repmgr_v2handshake_args *, u_int8_t *));
int __repmgr_v2handshake_unmarshal __P((ENV *, __repmgr_v2handshake_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_parm_refresh_marshal __P((ENV *, __repmgr_parm_refresh_args *, u_int8_t *));
int __repmgr_parm_refresh_unmarshal __P((ENV *, __repmgr_parm_refresh_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_permlsn_marshal __P((ENV *, __repmgr_permlsn_args *, u_int8_t *));
int __repmgr_permlsn_unmarshal __P((ENV *, __repmgr_permlsn_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_version_proposal_marshal __P((ENV *, __repmgr_version_proposal_args *, u_int8_t *));
int __repmgr_version_proposal_unmarshal __P((ENV *, __repmgr_version_proposal_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_version_confirmation_marshal __P((ENV *, __repmgr_version_confirmation_args *, u_int8_t *));
int __repmgr_version_confirmation_unmarshal __P((ENV *, __repmgr_version_confirmation_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_msg_hdr_marshal __P((ENV *, __repmgr_msg_hdr_args *, u_int8_t *));
int __repmgr_msg_hdr_unmarshal __P((ENV *, __repmgr_msg_hdr_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_msg_metadata_marshal __P((ENV *, __repmgr_msg_metadata_args *, u_int8_t *));
int __repmgr_msg_metadata_unmarshal __P((ENV *, __repmgr_msg_metadata_args *, u_int8_t *, size_t, u_int8_t **));
int __repmgr_membership_key_marshal __P((ENV *, __repmgr_membership_key_args *, u_int8_t *, size_t, size_t *));
int __repmgr_membership_key_unmarshal __P((ENV *, __repmgr_membership_key_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_membership_data_marshal __P((ENV *, __repmgr_membership_data_args *, u_int8_t *));
int __repmgr_membership_data_unmarshal __P((ENV *, __repmgr_membership_data_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_member_metadata_marshal __P((ENV *, __repmgr_member_metadata_args *, u_int8_t *));
int __repmgr_member_metadata_unmarshal __P((ENV *, __repmgr_member_metadata_args *, u_int8_t *, size_t, u_int8_t **));
int __repmgr_gm_fwd_marshal __P((ENV *, __repmgr_gm_fwd_args *, u_int8_t *, size_t, size_t *));
int __repmgr_gm_fwd_unmarshal __P((ENV *, __repmgr_gm_fwd_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_membr_vers_marshal __P((ENV *, __repmgr_membr_vers_args *, u_int8_t *));
int __repmgr_membr_vers_unmarshal __P((ENV *, __repmgr_membr_vers_args *, u_int8_t *, size_t, u_int8_t **));
int __repmgr_site_info_marshal __P((ENV *, __repmgr_site_info_args *, u_int8_t *, size_t, size_t *));
int __repmgr_site_info_unmarshal __P((ENV *, __repmgr_site_info_args *, u_int8_t *, size_t, u_int8_t **));
void __repmgr_connect_reject_marshal __P((ENV *, __repmgr_connect_reject_args *, u_int8_t *));
int __repmgr_connect_reject_unmarshal __P((ENV *, __repmgr_connect_reject_args *, u_int8_t *, size_t, u_int8_t **));
int __repmgr_member_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __repmgr_init_print __P((ENV *, DB_DISTAB *));
int __repmgr_init_election __P((ENV *, u_int32_t));
int __repmgr_claim_victory __P((ENV *));
int __repmgr_turn_on_elections __P((ENV *));
int __repmgr_start __P((DB_ENV *, int, u_int32_t));
int __repmgr_valid_config __P((ENV *, u_int32_t));
int __repmgr_autostart __P((ENV *));
int __repmgr_start_selector __P((ENV *));
int __repmgr_close __P((ENV *));
int __repmgr_stop __P((ENV *));
int __repmgr_set_ack_policy __P((DB_ENV *, int));
int __repmgr_get_ack_policy __P((DB_ENV *, int *));
int __repmgr_env_create __P((ENV *, DB_REP *));
void __repmgr_env_destroy __P((ENV *, DB_REP *));
int __repmgr_stop_threads __P((ENV *));
int __repmgr_local_site __P((DB_ENV *, DB_SITE **));
int __repmgr_channel __P((DB_ENV *, int, DB_CHANNEL **, u_int32_t));
int __repmgr_set_msg_dispatch __P((DB_ENV *, void (*)(DB_ENV *, DB_CHANNEL *, DBT *, u_int32_t, u_int32_t), u_int32_t));
int __repmgr_send_msg __P((DB_CHANNEL *, DBT *, u_int32_t, u_int32_t));
int __repmgr_send_request __P((DB_CHANNEL *, DBT *, u_int32_t, DBT *, db_timeout_t, u_int32_t));
int __repmgr_send_response __P((DB_CHANNEL *, DBT *, u_int32_t, u_int32_t));
int __repmgr_channel_close __P((DB_CHANNEL *, u_int32_t));
int __repmgr_channel_timeout __P((DB_CHANNEL *, db_timeout_t));
int __repmgr_send_request_inval __P((DB_CHANNEL *, DBT *, u_int32_t, DBT *, db_timeout_t, u_int32_t));
int __repmgr_channel_close_inval __P((DB_CHANNEL *, u_int32_t));
int __repmgr_channel_timeout_inval __P((DB_CHANNEL *, db_timeout_t));
int __repmgr_join_group __P((ENV *));
int __repmgr_site __P((DB_ENV *, const char *, u_int, DB_SITE **, u_int32_t));
int __repmgr_site_by_eid __P((DB_ENV *, int, DB_SITE **));
int __repmgr_get_site_address __P((DB_SITE *, const char **, u_int *));
int __repmgr_get_eid __P((DB_SITE *, int *));
int __repmgr_get_config __P((DB_SITE *, u_int32_t, u_int32_t *));
int __repmgr_site_config __P((DB_SITE *, u_int32_t, u_int32_t));
int __repmgr_site_close __P((DB_SITE *));
void *__repmgr_msg_thread __P((void *));
int __repmgr_send_err_resp __P((ENV *, CHANNEL *, int));
int __repmgr_handle_event __P((ENV *, u_int32_t, void *));
int __repmgr_update_membership __P((ENV *, DB_THREAD_INFO *, int, u_int32_t));
int __repmgr_set_gm_version __P((ENV *, DB_THREAD_INFO *, DB_TXN *, u_int32_t));
int __repmgr_setup_gmdb_op __P((ENV *, DB_THREAD_INFO *, DB_TXN **, u_int32_t));
int __repmgr_cleanup_gmdb_op __P((ENV *, int));
int __repmgr_hold_master_role __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_rlse_master_role __P((ENV *));
void __repmgr_set_sites __P((ENV *));
int __repmgr_connect __P((ENV *, repmgr_netaddr_t *, REPMGR_CONNECTION **, int *));
int __repmgr_send __P((DB_ENV *, const DBT *, const DBT *, const DB_LSN *, int, u_int32_t));
int __repmgr_sync_siteaddr __P((ENV *));
int __repmgr_send_broadcast __P((ENV *, u_int, const DBT *, const DBT *, u_int *, u_int *, int *));
int __repmgr_send_one __P((ENV *, REPMGR_CONNECTION *, u_int, const DBT *, const DBT *, db_timeout_t));
int __repmgr_send_many __P((ENV *, REPMGR_CONNECTION *, REPMGR_IOVECS *, db_timeout_t));
int __repmgr_send_own_msg __P((ENV *, REPMGR_CONNECTION *, u_int32_t, u_int8_t *, u_int32_t));
int __repmgr_write_iovecs __P((ENV *, REPMGR_CONNECTION *, REPMGR_IOVECS *, size_t *));
int __repmgr_bust_connection __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_disable_connection __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_cleanup_defunct __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_close_connection __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_decr_conn_ref __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_destroy_conn __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_pack_netaddr __P((ENV *, const char *, u_int, repmgr_netaddr_t *));
int __repmgr_getaddr __P((ENV *, const char *, u_int, int, ADDRINFO **));
int __repmgr_listen __P((ENV *));
int __repmgr_net_close __P((ENV *));
void __repmgr_net_destroy __P((ENV *, DB_REP *));
int __repmgr_thread_start __P((ENV *, REPMGR_RUNNABLE *));
int __repmgr_thread_join __P((REPMGR_RUNNABLE *));
int __repmgr_set_nonblock_conn __P((REPMGR_CONNECTION *));
int __repmgr_set_nonblocking __P((socket_t));
int __repmgr_wake_waiters __P((ENV *, waiter_t *));
int __repmgr_await_cond __P((ENV *, PREDICATE, void *, db_timeout_t, waiter_t *));
int __repmgr_await_gmdbop __P((ENV *));
void __repmgr_compute_wait_deadline __P((ENV*, struct timespec *, db_timeout_t));
int __repmgr_await_drain __P((ENV *, REPMGR_CONNECTION *, db_timeout_t));
int __repmgr_alloc_cond __P((cond_var_t *));
int __repmgr_free_cond __P((cond_var_t *));
void __repmgr_env_create_pf __P((DB_REP *));
int __repmgr_create_mutex_pf __P((mgr_mutex_t *));
int __repmgr_destroy_mutex_pf __P((mgr_mutex_t *));
int __repmgr_init __P((ENV *));
int __repmgr_deinit __P((ENV *));
int __repmgr_init_waiters __P((ENV *, waiter_t *));
int __repmgr_destroy_waiters __P((ENV *, waiter_t *));
int __repmgr_lock_mutex __P((mgr_mutex_t *));
int __repmgr_unlock_mutex __P((mgr_mutex_t *));
int __repmgr_signal __P((cond_var_t *));
int __repmgr_wake_msngers __P((ENV*, u_int));
int __repmgr_wake_main_thread __P((ENV*));
int __repmgr_writev __P((socket_t, db_iovec_t *, int, size_t *));
int __repmgr_readv __P((socket_t, db_iovec_t *, int, size_t *));
int __repmgr_select_loop __P((ENV *));
int __repmgr_queue_destroy __P((ENV *));
int __repmgr_queue_get __P((ENV *, REPMGR_MESSAGE **, REPMGR_RUNNABLE *));
int __repmgr_queue_put __P((ENV *, REPMGR_MESSAGE *));
int __repmgr_queue_size __P((ENV *));
int __repmgr_member_recover __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
void *__repmgr_select_thread __P((void *));
int __repmgr_bow_out __P((ENV *));
int __repmgr_accept __P((ENV *));
int __repmgr_compute_timeout __P((ENV *, db_timespec *));
REPMGR_SITE *__repmgr_connected_master __P((ENV *));
int __repmgr_check_timeouts __P((ENV *));
int __repmgr_first_try_connections __P((ENV *));
int __repmgr_send_v1_handshake __P((ENV *, REPMGR_CONNECTION *, void *, size_t));
int __repmgr_read_from_site __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_read_conn __P((REPMGR_CONNECTION *));
int __repmgr_prepare_simple_input __P((ENV *, REPMGR_CONNECTION *, __repmgr_msg_hdr_args *));
int __repmgr_send_handshake __P((ENV *, REPMGR_CONNECTION *, void *, size_t, u_int32_t));
int __repmgr_find_version_info __P((ENV *, REPMGR_CONNECTION *, DBT *));
int __repmgr_write_some __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_stat_pp __P((DB_ENV *, DB_REPMGR_STAT **, u_int32_t));
int __repmgr_stat_print_pp __P((DB_ENV *, u_int32_t));
int __repmgr_stat_print __P((ENV *, u_int32_t));
int __repmgr_site_list __P((DB_ENV *, u_int *, DB_REPMGR_SITE **));
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_close __P((ENV *));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_get_ack_policy __P((DB_ENV *, int *));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_set_ack_policy __P((DB_ENV *, int));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_site __P((DB_ENV *, const char *, u_int, DB_SITE **, u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_site_by_eid __P((DB_ENV *, int, DB_SITE **));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_local_site __P((DB_ENV *, DB_SITE **));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_site_list __P((DB_ENV *, u_int *, DB_REPMGR_SITE **));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_start __P((DB_ENV *, int, u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_stat_pp __P((DB_ENV *, DB_REPMGR_STAT **, u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_stat_print_pp __P((DB_ENV *, u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_handle_event __P((ENV *, u_int32_t, void *));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_channel __P((DB_ENV *, int, DB_CHANNEL **, u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_set_msg_dispatch __P((DB_ENV *, void (*)(DB_ENV *, DB_CHANNEL *, DBT *, u_int32_t, u_int32_t), u_int32_t));
#endif
#ifndef HAVE_REPLICATION_THREADS
int __repmgr_init_recover __P((ENV *, DB_DISTAB *));
#endif
int __repmgr_schedule_connection_attempt __P((ENV *, int, int));
int __repmgr_is_server __P((ENV *, REPMGR_SITE *));
void __repmgr_reset_for_reading __P((REPMGR_CONNECTION *));
int __repmgr_new_connection __P((ENV *, REPMGR_CONNECTION **, socket_t, int));
int __repmgr_set_keepalive __P((ENV *, REPMGR_CONNECTION *));
int __repmgr_new_site __P((ENV *, REPMGR_SITE**, const char *, u_int));
int __repmgr_create_mutex __P((ENV *, mgr_mutex_t **));
int __repmgr_destroy_mutex __P((ENV *, mgr_mutex_t *));
void __repmgr_cleanup_netaddr __P((ENV *, repmgr_netaddr_t *));
void __repmgr_iovec_init __P((REPMGR_IOVECS *));
void __repmgr_add_buffer __P((REPMGR_IOVECS *, void *, size_t));
void __repmgr_add_dbt __P((REPMGR_IOVECS *, const DBT *));
int __repmgr_update_consumed __P((REPMGR_IOVECS *, size_t));
int __repmgr_prepare_my_addr __P((ENV *, DBT *));
int __repmgr_get_nsites __P((ENV *, u_int32_t *));
int __repmgr_thread_failure __P((ENV *, int));
char *__repmgr_format_eid_loc __P((DB_REP *, REPMGR_CONNECTION *, char *));
char *__repmgr_format_site_loc __P((REPMGR_SITE *, char *));
char *__repmgr_format_addr_loc __P((repmgr_netaddr_t *, char *));
int __repmgr_repstart __P((ENV *, u_int32_t));
int __repmgr_become_master __P((ENV *));
int __repmgr_each_connection __P((ENV *, CONNECTION_ACTION, void *, int));
int __repmgr_open __P((ENV *, void *));
int __repmgr_join __P((ENV *, void *));
int __repmgr_env_refresh __P((ENV *env));
int __repmgr_share_netaddrs __P((ENV *, void *, u_int, u_int));
int __repmgr_copy_in_added_sites __P((ENV *));
int __repmgr_init_new_sites __P((ENV *, int, int));
int __repmgr_failchk __P((ENV *));
int __repmgr_master_is_known __P((ENV *));
int __repmgr_stable_lsn __P((ENV *, DB_LSN *));
int __repmgr_send_sync_msg __P((ENV *, REPMGR_CONNECTION *, u_int32_t, u_int8_t *, u_int32_t));
int __repmgr_marshal_member_list __P((ENV *, u_int8_t **, size_t *));
int __repmgr_refresh_membership __P((ENV *, u_int8_t *, size_t));
int __repmgr_reload_gmdb __P((ENV *));
int __repmgr_gmdb_version_cmp __P((ENV *, u_int32_t, u_int32_t));
int __repmgr_init_save __P((ENV *, DBT *));
int __repmgr_init_restore __P((ENV *, DBT *));
int __repmgr_defer_op __P((ENV *, u_int32_t));
void __repmgr_fire_conn_err_event __P((ENV *, REPMGR_CONNECTION *, int));
void __repmgr_print_conn_err __P((ENV *, repmgr_netaddr_t *, int));
int __repmgr_become_client __P((ENV *));
REPMGR_SITE *__repmgr_lookup_site __P((ENV *, const char *, u_int));
int __repmgr_find_site __P((ENV *, const char *, u_int, int *));
int __repmgr_set_membership __P((ENV *, const char *, u_int, u_int32_t));
int __repmgr_bcast_parm_refresh __P((ENV *));
int __repmgr_chg_prio __P((ENV *, u_int32_t, u_int32_t));
int __repmgr_bcast_own_msg __P((ENV *, u_int32_t, u_int8_t *, size_t));

#if defined(__cplusplus)
}
#endif
#endif /* !_repmgr_ext_h_ */
