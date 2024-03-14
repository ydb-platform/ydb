/*--------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration scheme, including SET
 * command, configuration file, and command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2021, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif
#include <unistd.h>

#include "access/commit_ts.h"
#include "access/gin.h"
#include "access/rmgr.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "commands/variable.h"
#include "common/string.h"
#include "funcapi.h"
#include "jit/jit.h"
#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "postmaster/walwriter.h"
#include "replication/logicallauncher.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/dsm_impl.h"
#include "storage/fd.h"
#include "storage/large_object.h"
#include "storage/pg_shmem.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/standby.h"
#include "tcop/tcopprot.h"
#include "tsearch/ts_cache.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/float.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/pg_lsn.h"
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/queryjumble.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/tzparser.h"
#include "utils/inval.h"
#include "utils/varlena.h"
#include "utils/xml.h"

#ifndef PG_KRB_SRVTAB
#define PG_KRB_SRVTAB ""
#endif

#define CONFIG_FILENAME "postgresql.conf"
#define HBA_FILENAME	"pg_hba.conf"
#define IDENT_FILENAME	"pg_ident.conf"

#ifdef EXEC_BACKEND
#define CONFIG_EXEC_PARAMS "global/config_exec_params"
#define CONFIG_EXEC_PARAMS_NEW "global/config_exec_params.new"
#endif

/*
 * Precision with which REAL type guc values are to be printed for GUC
 * serialization.
 */
#define REALTYPE_PRECISION 17

/* XXX these should appear in other modules' header files */
extern __thread bool Log_disconnections;
extern __thread int	CommitDelay;
extern __thread int	CommitSiblings;
extern __thread char *default_tablespace;
extern __thread char *temp_tablespaces;
extern __thread bool ignore_checksum_failure;
extern __thread bool ignore_invalid_pages;
extern __thread bool synchronize_seqscans;

#ifdef TRACE_SYNCSCAN
extern bool trace_syncscan;
#endif
#ifdef DEBUG_BOUNDED_SORT
extern bool optimize_bounded_sort;
#endif

static __thread int	GUC_check_errcode_value;

/* global variables for check hook support */
__thread char	   *GUC_check_errmsg_string;
__thread char	   *GUC_check_errdetail_string;
__thread char	   *GUC_check_errhint_string;

static void do_serialize(char **destptr, Size *maxbytes, const char *fmt,...) pg_attribute_printf(3, 4);

static void set_config_sourcefile(const char *name, char *sourcefile,
								  int sourceline);
static bool call_bool_check_hook(struct config_bool *conf, bool *newval,
								 void **extra, GucSource source, int elevel);
static bool call_int_check_hook(struct config_int *conf, int *newval,
								void **extra, GucSource source, int elevel);
static bool call_real_check_hook(struct config_real *conf, double *newval,
								 void **extra, GucSource source, int elevel);
static bool call_string_check_hook(struct config_string *conf, char **newval,
								   void **extra, GucSource source, int elevel);
static bool call_enum_check_hook(struct config_enum *conf, int *newval,
								 void **extra, GucSource source, int elevel);

static bool check_log_destination(char **newval, void **extra, GucSource source);
static void assign_log_destination(const char *newval, void *extra);

static bool check_wal_consistency_checking(char **newval, void **extra,
										   GucSource source);
static void assign_wal_consistency_checking(const char *newval, void *extra);

#ifdef HAVE_SYSLOG
static __thread int	syslog_facility = LOG_LOCAL0;
#else
static __thread int	syslog_facility = 0;
#endif

static void assign_syslog_facility(int newval, void *extra);
static void assign_syslog_ident(const char *newval, void *extra);
static void assign_session_replication_role(int newval, void *extra);
static bool check_temp_buffers(int *newval, void **extra, GucSource source);
static bool check_bonjour(bool *newval, void **extra, GucSource source);
static bool check_ssl(bool *newval, void **extra, GucSource source);
static bool check_stage_log_stats(bool *newval, void **extra, GucSource source);
static bool check_log_stats(bool *newval, void **extra, GucSource source);
static bool check_canonical_path(char **newval, void **extra, GucSource source);
static bool check_timezone_abbreviations(char **newval, void **extra, GucSource source);
static void assign_timezone_abbreviations(const char *newval, void *extra);
static void pg_timezone_abbrev_initialize(void);
static const char *show_archive_command(void);
static void assign_tcp_keepalives_idle(int newval, void *extra);
static void assign_tcp_keepalives_interval(int newval, void *extra);
static void assign_tcp_keepalives_count(int newval, void *extra);
static void assign_tcp_user_timeout(int newval, void *extra);
static const char *show_tcp_keepalives_idle(void);
static const char *show_tcp_keepalives_interval(void);
static const char *show_tcp_keepalives_count(void);
static const char *show_tcp_user_timeout(void);
static bool check_maxconnections(int *newval, void **extra, GucSource source);
static bool check_max_worker_processes(int *newval, void **extra, GucSource source);
static bool check_autovacuum_max_workers(int *newval, void **extra, GucSource source);
static bool check_max_wal_senders(int *newval, void **extra, GucSource source);
static bool check_autovacuum_work_mem(int *newval, void **extra, GucSource source);
static bool check_effective_io_concurrency(int *newval, void **extra, GucSource source);
static bool check_maintenance_io_concurrency(int *newval, void **extra, GucSource source);
static bool check_huge_page_size(int *newval, void **extra, GucSource source);
static bool check_client_connection_check_interval(int *newval, void **extra, GucSource source);
static void assign_pgstat_temp_directory(const char *newval, void *extra);
static bool check_application_name(char **newval, void **extra, GucSource source);
static void assign_application_name(const char *newval, void *extra);
static bool check_cluster_name(char **newval, void **extra, GucSource source);
static const char *show_unix_socket_permissions(void);
static const char *show_log_file_mode(void);
static const char *show_data_directory_mode(void);
static const char *show_in_hot_standby(void);
static bool check_backtrace_functions(char **newval, void **extra, GucSource source);
static void assign_backtrace_functions(const char *newval, void *extra);
static bool check_recovery_target_timeline(char **newval, void **extra, GucSource source);
static void assign_recovery_target_timeline(const char *newval, void *extra);
static bool check_recovery_target(char **newval, void **extra, GucSource source);
static void assign_recovery_target(const char *newval, void *extra);
static bool check_recovery_target_xid(char **newval, void **extra, GucSource source);
static void assign_recovery_target_xid(const char *newval, void *extra);
static bool check_recovery_target_time(char **newval, void **extra, GucSource source);
static void assign_recovery_target_time(const char *newval, void *extra);
static bool check_recovery_target_name(char **newval, void **extra, GucSource source);
static void assign_recovery_target_name(const char *newval, void *extra);
static bool check_recovery_target_lsn(char **newval, void **extra, GucSource source);
static void assign_recovery_target_lsn(const char *newval, void *extra);
static bool check_primary_slot_name(char **newval, void **extra, GucSource source);
static bool check_default_with_oids(bool *newval, void **extra, GucSource source);

/* Private functions in guc-file.l that need to be called from guc.c */
static ConfigVariable *ProcessConfigFileInternal(GucContext context,
												 bool applySettings, int elevel);


/*
 * Options for enum values defined in this module.
 *
 * NOTE! Option values may not contain double quotes!
 */

static const struct config_enum_entry bytea_output_options[] = {
	{"escape", BYTEA_OUTPUT_ESCAPE, false},
	{"hex", BYTEA_OUTPUT_HEX, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(bytea_output_options) == (BYTEA_OUTPUT_HEX + 2),
				 "array length mismatch");

/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level), and because "fatal"/"panic"
 * aren't sensible for client_min_messages.
 */
static const struct config_enum_entry client_message_level_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"log", LOG, false},
	{"info", INFO, true},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{NULL, 0, false}
};

static const struct config_enum_entry server_message_level_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"log", LOG, false},
	{"fatal", FATAL, false},
	{"panic", PANIC, false},
	{NULL, 0, false}
};

static const struct config_enum_entry intervalstyle_options[] = {
	{"postgres", INTSTYLE_POSTGRES, false},
	{"postgres_verbose", INTSTYLE_POSTGRES_VERBOSE, false},
	{"sql_standard", INTSTYLE_SQL_STANDARD, false},
	{"iso_8601", INTSTYLE_ISO_8601, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(intervalstyle_options) == (INTSTYLE_ISO_8601 + 2),
				 "array length mismatch");

static const struct config_enum_entry log_error_verbosity_options[] = {
	{"terse", PGERROR_TERSE, false},
	{"default", PGERROR_DEFAULT, false},
	{"verbose", PGERROR_VERBOSE, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(log_error_verbosity_options) == (PGERROR_VERBOSE + 2),
				 "array length mismatch");

static const struct config_enum_entry log_statement_options[] = {
	{"none", LOGSTMT_NONE, false},
	{"ddl", LOGSTMT_DDL, false},
	{"mod", LOGSTMT_MOD, false},
	{"all", LOGSTMT_ALL, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(log_statement_options) == (LOGSTMT_ALL + 2),
				 "array length mismatch");

static const struct config_enum_entry isolation_level_options[] = {
	{"serializable", XACT_SERIALIZABLE, false},
	{"repeatable read", XACT_REPEATABLE_READ, false},
	{"read committed", XACT_READ_COMMITTED, false},
	{"read uncommitted", XACT_READ_UNCOMMITTED, false},
	{NULL, 0}
};

static const struct config_enum_entry session_replication_role_options[] = {
	{"origin", SESSION_REPLICATION_ROLE_ORIGIN, false},
	{"replica", SESSION_REPLICATION_ROLE_REPLICA, false},
	{"local", SESSION_REPLICATION_ROLE_LOCAL, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(session_replication_role_options) == (SESSION_REPLICATION_ROLE_LOCAL + 2),
				 "array length mismatch");

static const struct config_enum_entry syslog_facility_options[] = {
#ifdef HAVE_SYSLOG
	{"local0", LOG_LOCAL0, false},
	{"local1", LOG_LOCAL1, false},
	{"local2", LOG_LOCAL2, false},
	{"local3", LOG_LOCAL3, false},
	{"local4", LOG_LOCAL4, false},
	{"local5", LOG_LOCAL5, false},
	{"local6", LOG_LOCAL6, false},
	{"local7", LOG_LOCAL7, false},
#else
	{"none", 0, false},
#endif
	{NULL, 0}
};

static const struct config_enum_entry track_function_options[] = {
	{"none", TRACK_FUNC_OFF, false},
	{"pl", TRACK_FUNC_PL, false},
	{"all", TRACK_FUNC_ALL, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(track_function_options) == (TRACK_FUNC_ALL + 2),
				 "array length mismatch");

static const struct config_enum_entry xmlbinary_options[] = {
	{"base64", XMLBINARY_BASE64, false},
	{"hex", XMLBINARY_HEX, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(xmlbinary_options) == (XMLBINARY_HEX + 2),
				 "array length mismatch");

static const struct config_enum_entry xmloption_options[] = {
	{"content", XMLOPTION_CONTENT, false},
	{"document", XMLOPTION_DOCUMENT, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(xmloption_options) == (XMLOPTION_CONTENT + 2),
				 "array length mismatch");

/*
 * Although only "on", "off", and "safe_encoding" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const const struct config_enum_entry backslash_quote_options[] = {
	{"safe_encoding", BACKSLASH_QUOTE_SAFE_ENCODING, false},
	{"on", BACKSLASH_QUOTE_ON, false},
	{"off", BACKSLASH_QUOTE_OFF, false},
	{"true", BACKSLASH_QUOTE_ON, true},
	{"false", BACKSLASH_QUOTE_OFF, true},
	{"yes", BACKSLASH_QUOTE_ON, true},
	{"no", BACKSLASH_QUOTE_OFF, true},
	{"1", BACKSLASH_QUOTE_ON, true},
	{"0", BACKSLASH_QUOTE_OFF, true},
	{NULL, 0, false}
};

/*
 * Although only "on", "off", and "auto" are documented, we accept
 * all the likely variants of "on" and "off".
 */
static const struct config_enum_entry compute_query_id_options[] = {
	{"auto", COMPUTE_QUERY_ID_AUTO, false},
	{"on", COMPUTE_QUERY_ID_ON, false},
	{"off", COMPUTE_QUERY_ID_OFF, false},
	{"true", COMPUTE_QUERY_ID_ON, true},
	{"false", COMPUTE_QUERY_ID_OFF, true},
	{"yes", COMPUTE_QUERY_ID_ON, true},
	{"no", COMPUTE_QUERY_ID_OFF, true},
	{"1", COMPUTE_QUERY_ID_ON, true},
	{"0", COMPUTE_QUERY_ID_OFF, true},
	{NULL, 0, false}
};

/*
 * Although only "on", "off", and "partition" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry constraint_exclusion_options[] = {
	{"partition", CONSTRAINT_EXCLUSION_PARTITION, false},
	{"on", CONSTRAINT_EXCLUSION_ON, false},
	{"off", CONSTRAINT_EXCLUSION_OFF, false},
	{"true", CONSTRAINT_EXCLUSION_ON, true},
	{"false", CONSTRAINT_EXCLUSION_OFF, true},
	{"yes", CONSTRAINT_EXCLUSION_ON, true},
	{"no", CONSTRAINT_EXCLUSION_OFF, true},
	{"1", CONSTRAINT_EXCLUSION_ON, true},
	{"0", CONSTRAINT_EXCLUSION_OFF, true},
	{NULL, 0, false}
};

/*
 * Although only "on", "off", "remote_apply", "remote_write", and "local" are
 * documented, we accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry synchronous_commit_options[] = {
	{"local", SYNCHRONOUS_COMMIT_LOCAL_FLUSH, false},
	{"remote_write", SYNCHRONOUS_COMMIT_REMOTE_WRITE, false},
	{"remote_apply", SYNCHRONOUS_COMMIT_REMOTE_APPLY, false},
	{"on", SYNCHRONOUS_COMMIT_ON, false},
	{"off", SYNCHRONOUS_COMMIT_OFF, false},
	{"true", SYNCHRONOUS_COMMIT_ON, true},
	{"false", SYNCHRONOUS_COMMIT_OFF, true},
	{"yes", SYNCHRONOUS_COMMIT_ON, true},
	{"no", SYNCHRONOUS_COMMIT_OFF, true},
	{"1", SYNCHRONOUS_COMMIT_ON, true},
	{"0", SYNCHRONOUS_COMMIT_OFF, true},
	{NULL, 0, false}
};

/*
 * Although only "on", "off", "try" are documented, we accept all the likely
 * variants of "on" and "off".
 */
static const struct config_enum_entry huge_pages_options[] = {
	{"off", HUGE_PAGES_OFF, false},
	{"on", HUGE_PAGES_ON, false},
	{"try", HUGE_PAGES_TRY, false},
	{"true", HUGE_PAGES_ON, true},
	{"false", HUGE_PAGES_OFF, true},
	{"yes", HUGE_PAGES_ON, true},
	{"no", HUGE_PAGES_OFF, true},
	{"1", HUGE_PAGES_ON, true},
	{"0", HUGE_PAGES_OFF, true},
	{NULL, 0, false}
};

static const struct config_enum_entry force_parallel_mode_options[] = {
	{"off", FORCE_PARALLEL_OFF, false},
	{"on", FORCE_PARALLEL_ON, false},
	{"regress", FORCE_PARALLEL_REGRESS, false},
	{"true", FORCE_PARALLEL_ON, true},
	{"false", FORCE_PARALLEL_OFF, true},
	{"yes", FORCE_PARALLEL_ON, true},
	{"no", FORCE_PARALLEL_OFF, true},
	{"1", FORCE_PARALLEL_ON, true},
	{"0", FORCE_PARALLEL_OFF, true},
	{NULL, 0, false}
};

static const struct config_enum_entry plan_cache_mode_options[] = {
	{"auto", PLAN_CACHE_MODE_AUTO, false},
	{"force_generic_plan", PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, false},
	{"force_custom_plan", PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN, false},
	{NULL, 0, false}
};

static const struct config_enum_entry password_encryption_options[] = {
	{"md5", PASSWORD_TYPE_MD5, false},
	{"scram-sha-256", PASSWORD_TYPE_SCRAM_SHA_256, false},
	{NULL, 0, false}
};

const struct config_enum_entry ssl_protocol_versions_info[] = {
	{"", PG_TLS_ANY, false},
	{"TLSv1", PG_TLS1_VERSION, false},
	{"TLSv1.1", PG_TLS1_1_VERSION, false},
	{"TLSv1.2", PG_TLS1_2_VERSION, false},
	{"TLSv1.3", PG_TLS1_3_VERSION, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(ssl_protocol_versions_info) == (PG_TLS1_3_VERSION + 2),
				 "array length mismatch");

static __thread struct config_enum_entry recovery_init_sync_method_options[] = {
	{"fsync", RECOVERY_INIT_SYNC_METHOD_FSYNC, false},
#ifdef HAVE_SYNCFS
	{"syncfs", RECOVERY_INIT_SYNC_METHOD_SYNCFS, false},
#endif
	{NULL, 0, false}
};

static __thread struct config_enum_entry shared_memory_options[] = {
#ifndef WIN32
	{"sysv", SHMEM_TYPE_SYSV, false},
#endif
#ifndef EXEC_BACKEND
	{"mmap", SHMEM_TYPE_MMAP, false},
#endif
#ifdef WIN32
	{"windows", SHMEM_TYPE_WINDOWS, false},
#endif
	{NULL, 0, false}
};

static __thread struct config_enum_entry default_toast_compression_options[] = {
	{"pglz", TOAST_PGLZ_COMPRESSION, false},
#ifdef  USE_LZ4
	{"lz4", TOAST_LZ4_COMPRESSION, false},
#endif
	{NULL, 0, false}
};

/*
 * Options for enum values stored in other modules
 */
extern const struct config_enum_entry wal_level_options[];
extern const struct config_enum_entry archive_mode_options[];
extern const struct config_enum_entry recovery_target_action_options[];
extern const struct config_enum_entry sync_method_options[];
extern const struct config_enum_entry dynamic_shared_memory_options[];

/*
 * GUC option variables that are exported from this module
 */
__thread bool		log_duration = false;
__thread bool		Debug_print_plan = false;
__thread bool		Debug_print_parse = false;
__thread bool		Debug_print_rewritten = false;
__thread bool		Debug_pretty_print = true;

__thread bool		log_parser_stats = false;
__thread bool		log_planner_stats = false;
__thread bool		log_executor_stats = false;
__thread bool		log_statement_stats = false;	/* this is sort of all three above
											 * together */
__thread bool		log_btree_build_stats = false;
__thread char	   *event_source;

__thread bool		row_security;
__thread bool		check_function_bodies = true;

/*
 * This GUC exists solely for backward compatibility, check its definition for
 * details.
 */
__thread bool		default_with_oids = false;
__thread bool		session_auth_is_superuser;

__thread int			log_min_error_statement = ERROR;
__thread int			log_min_messages = WARNING;
__thread int			client_min_messages = NOTICE;
__thread int			log_min_duration_sample = -1;
__thread int			log_min_duration_statement = -1;
__thread int			log_parameter_max_length = -1;
__thread int			log_parameter_max_length_on_error = 0;
__thread int			log_temp_files = -1;
__thread double		log_statement_sample_rate = 1.0;
__thread double		log_xact_sample_rate = 0;
__thread int			trace_recovery_messages = LOG;
__thread char	   *backtrace_functions;
__thread char	   *backtrace_symbol_list;

__thread int			temp_file_limit = -1;

__thread int			num_temp_buffers = 1024;

__thread char	   *cluster_name = "";
__thread char	   *ConfigFileName;
__thread char	   *HbaFileName;
__thread char	   *IdentFileName;
__thread char	   *external_pid_file;

__thread char	   *pgstat_temp_directory;

__thread char	   *application_name;

__thread int			tcp_keepalives_idle;
__thread int			tcp_keepalives_interval;
__thread int			tcp_keepalives_count;
__thread int			tcp_user_timeout;

/*
 * SSL renegotiation was been removed in PostgreSQL 9.5, but we tolerate it
 * being set to zero (meaning never renegotiate) for backward compatibility.
 * This avoids breaking compatibility with clients that have never supported
 * renegotiation and therefore always try to zero it.
 */
__thread int			ssl_renegotiation_limit;

/*
 * This really belongs in pg_shmem.c, but is defined here so that it doesn't
 * need to be duplicated in all the different implementations of pg_shmem.c.
 */
__thread int			huge_pages;
__thread int			huge_page_size;

/*
 * These variables are all dummies that don't do anything, except in some
 * cases provide the value for SHOW to display.  The real state is elsewhere
 * and is kept in sync by assign_hooks.
 */
static __thread char *syslog_ident_str;
static __thread double phony_random_seed;
static __thread char *client_encoding_string;
static __thread char *datestyle_string;
static __thread char *locale_collate;
static __thread char *locale_ctype;
static __thread char *server_encoding_string;
static __thread char *server_version_string;
static __thread int	server_version_num;
static __thread char *timezone_string;
static __thread char *log_timezone_string;
static __thread char *timezone_abbreviations_string;
static __thread char *data_directory;
static __thread char *session_authorization_string;
static __thread int	max_function_args;
static __thread int	max_index_keys;
static __thread int	max_identifier_length;
static __thread int	block_size;
static __thread int	segment_size;
static __thread int	wal_block_size;
static __thread bool data_checksums;
static __thread bool integer_datetimes;
static __thread bool assert_enabled;
static __thread bool in_hot_standby;
static __thread char *recovery_target_timeline_string;
static __thread char *recovery_target_string;
static __thread char *recovery_target_xid_string;
static __thread char *recovery_target_name_string;
static __thread char *recovery_target_lsn_string;


/* should be static, but commands/variable.c needs to get at this */
__thread char	   *role_string;


/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */
const char *const GucContext_Names[] =
{
	 /* PGC_INTERNAL */ "internal",
	 /* PGC_POSTMASTER */ "postmaster",
	 /* PGC_SIGHUP */ "sighup",
	 /* PGC_SU_BACKEND */ "superuser-backend",
	 /* PGC_BACKEND */ "backend",
	 /* PGC_SUSET */ "superuser",
	 /* PGC_USERSET */ "user"
};

StaticAssertDecl(lengthof(GucContext_Names) == (PGC_USERSET + 1),
				 "array length mismatch");

/*
 * Displayable names for source types (enum GucSource)
 *
 * Note: these strings are deliberately not localized.
 */
const char *const GucSource_Names[] =
{
	 /* PGC_S_DEFAULT */ "default",
	 /* PGC_S_DYNAMIC_DEFAULT */ "default",
	 /* PGC_S_ENV_VAR */ "environment variable",
	 /* PGC_S_FILE */ "configuration file",
	 /* PGC_S_ARGV */ "command line",
	 /* PGC_S_GLOBAL */ "global",
	 /* PGC_S_DATABASE */ "database",
	 /* PGC_S_USER */ "user",
	 /* PGC_S_DATABASE_USER */ "database user",
	 /* PGC_S_CLIENT */ "client",
	 /* PGC_S_OVERRIDE */ "override",
	 /* PGC_S_INTERACTIVE */ "interactive",
	 /* PGC_S_TEST */ "test",
	 /* PGC_S_SESSION */ "session"
};

StaticAssertDecl(lengthof(GucSource_Names) == (PGC_S_SESSION + 1),
				 "array length mismatch");

/*
 * Displayable names for the groupings defined in enum config_group
 */
const char *const config_group_names[] =
{
	/* UNGROUPED */
	gettext_noop("Ungrouped"),
	/* FILE_LOCATIONS */
	gettext_noop("File Locations"),
	/* CONN_AUTH_SETTINGS */
	gettext_noop("Connections and Authentication / Connection Settings"),
	/* CONN_AUTH_AUTH */
	gettext_noop("Connections and Authentication / Authentication"),
	/* CONN_AUTH_SSL */
	gettext_noop("Connections and Authentication / SSL"),
	/* RESOURCES_MEM */
	gettext_noop("Resource Usage / Memory"),
	/* RESOURCES_DISK */
	gettext_noop("Resource Usage / Disk"),
	/* RESOURCES_KERNEL */
	gettext_noop("Resource Usage / Kernel Resources"),
	/* RESOURCES_VACUUM_DELAY */
	gettext_noop("Resource Usage / Cost-Based Vacuum Delay"),
	/* RESOURCES_BGWRITER */
	gettext_noop("Resource Usage / Background Writer"),
	/* RESOURCES_ASYNCHRONOUS */
	gettext_noop("Resource Usage / Asynchronous Behavior"),
	/* WAL_SETTINGS */
	gettext_noop("Write-Ahead Log / Settings"),
	/* WAL_CHECKPOINTS */
	gettext_noop("Write-Ahead Log / Checkpoints"),
	/* WAL_ARCHIVING */
	gettext_noop("Write-Ahead Log / Archiving"),
	/* WAL_ARCHIVE_RECOVERY */
	gettext_noop("Write-Ahead Log / Archive Recovery"),
	/* WAL_RECOVERY_TARGET */
	gettext_noop("Write-Ahead Log / Recovery Target"),
	/* REPLICATION_SENDING */
	gettext_noop("Replication / Sending Servers"),
	/* REPLICATION_PRIMARY */
	gettext_noop("Replication / Primary Server"),
	/* REPLICATION_STANDBY */
	gettext_noop("Replication / Standby Servers"),
	/* REPLICATION_SUBSCRIBERS */
	gettext_noop("Replication / Subscribers"),
	/* QUERY_TUNING_METHOD */
	gettext_noop("Query Tuning / Planner Method Configuration"),
	/* QUERY_TUNING_COST */
	gettext_noop("Query Tuning / Planner Cost Constants"),
	/* QUERY_TUNING_GEQO */
	gettext_noop("Query Tuning / Genetic Query Optimizer"),
	/* QUERY_TUNING_OTHER */
	gettext_noop("Query Tuning / Other Planner Options"),
	/* LOGGING_WHERE */
	gettext_noop("Reporting and Logging / Where to Log"),
	/* LOGGING_WHEN */
	gettext_noop("Reporting and Logging / When to Log"),
	/* LOGGING_WHAT */
	gettext_noop("Reporting and Logging / What to Log"),
	/* PROCESS_TITLE */
	gettext_noop("Reporting and Logging / Process Title"),
	/* STATS_MONITORING */
	gettext_noop("Statistics / Monitoring"),
	/* STATS_COLLECTOR */
	gettext_noop("Statistics / Query and Index Statistics Collector"),
	/* AUTOVACUUM */
	gettext_noop("Autovacuum"),
	/* CLIENT_CONN_STATEMENT */
	gettext_noop("Client Connection Defaults / Statement Behavior"),
	/* CLIENT_CONN_LOCALE */
	gettext_noop("Client Connection Defaults / Locale and Formatting"),
	/* CLIENT_CONN_PRELOAD */
	gettext_noop("Client Connection Defaults / Shared Library Preloading"),
	/* CLIENT_CONN_OTHER */
	gettext_noop("Client Connection Defaults / Other Defaults"),
	/* LOCK_MANAGEMENT */
	gettext_noop("Lock Management"),
	/* COMPAT_OPTIONS_PREVIOUS */
	gettext_noop("Version and Platform Compatibility / Previous PostgreSQL Versions"),
	/* COMPAT_OPTIONS_CLIENT */
	gettext_noop("Version and Platform Compatibility / Other Platforms and Clients"),
	/* ERROR_HANDLING */
	gettext_noop("Error Handling"),
	/* PRESET_OPTIONS */
	gettext_noop("Preset Options"),
	/* CUSTOM_OPTIONS */
	gettext_noop("Customized Options"),
	/* DEVELOPER_OPTIONS */
	gettext_noop("Developer Options"),
	/* help_config wants this array to be null-terminated */
	NULL
};

StaticAssertDecl(lengthof(config_group_names) == (DEVELOPER_OPTIONS + 2),
				 "array length mismatch");

/*
 * Displayable names for GUC variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */
const char *const config_type_names[] =
{
	 /* PGC_BOOL */ "bool",
	 /* PGC_INT */ "integer",
	 /* PGC_REAL */ "real",
	 /* PGC_STRING */ "string",
	 /* PGC_ENUM */ "enum"
};

StaticAssertDecl(lengthof(config_type_names) == (PGC_ENUM + 1),
				 "array length mismatch");

/*
 * Unit conversion tables.
 *
 * There are two tables, one for memory units, and another for time units.
 * For each supported conversion from one unit to another, we have an entry
 * in the table.
 *
 * To keep things simple, and to avoid possible roundoff error,
 * conversions are never chained.  There needs to be a direct conversion
 * between all units (of the same type).
 *
 * The conversions for each base unit must be kept in order from greatest to
 * smallest human-friendly unit; convert_xxx_from_base_unit() rely on that.
 * (The order of the base-unit groups does not matter.)
 */
#define MAX_UNIT_LEN		3	/* length of longest recognized unit string */

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* GUC_UNIT_XXX */
	double		multiplier;		/* Factor for converting unit -> base_unit */
} unit_conversion;

/* Ensure that the constants in the tables don't overflow or underflow */
#if BLCKSZ < 1024 || BLCKSZ > (1024*1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024*1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif

static __thread const char *memory_units_hint = gettext_noop("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".");

static const unit_conversion memory_unit_conversion_table[] =
{
	{"TB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0},
	{"MB", GUC_UNIT_BYTE, 1024.0 * 1024.0},
	{"kB", GUC_UNIT_BYTE, 1024.0},
	{"B", GUC_UNIT_BYTE, 1.0},

	{"TB", GUC_UNIT_KB, 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_KB, 1024.0 * 1024.0},
	{"MB", GUC_UNIT_KB, 1024.0},
	{"kB", GUC_UNIT_KB, 1.0},
	{"B", GUC_UNIT_KB, 1.0 / 1024.0},

	{"TB", GUC_UNIT_MB, 1024.0 * 1024.0},
	{"GB", GUC_UNIT_MB, 1024.0},
	{"MB", GUC_UNIT_MB, 1.0},
	{"kB", GUC_UNIT_MB, 1.0 / 1024.0},
	{"B", GUC_UNIT_MB, 1.0 / (1024.0 * 1024.0)},

	{"TB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"GB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"MB", GUC_UNIT_BLOCKS, 1024.0 / (BLCKSZ / 1024)},
	{"kB", GUC_UNIT_BLOCKS, 1.0 / (BLCKSZ / 1024)},
	{"B", GUC_UNIT_BLOCKS, 1.0 / BLCKSZ},

	{"TB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"GB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"MB", GUC_UNIT_XBLOCKS, 1024.0 / (XLOG_BLCKSZ / 1024)},
	{"kB", GUC_UNIT_XBLOCKS, 1.0 / (XLOG_BLCKSZ / 1024)},
	{"B", GUC_UNIT_XBLOCKS, 1.0 / XLOG_BLCKSZ},

	{""}						/* end of table marker */
};

static __thread const char *time_units_hint = gettext_noop("Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".");

static const unit_conversion time_unit_conversion_table[] =
{
	{"d", GUC_UNIT_MS, 1000 * 60 * 60 * 24},
	{"h", GUC_UNIT_MS, 1000 * 60 * 60},
	{"min", GUC_UNIT_MS, 1000 * 60},
	{"s", GUC_UNIT_MS, 1000},
	{"ms", GUC_UNIT_MS, 1},
	{"us", GUC_UNIT_MS, 1.0 / 1000},

	{"d", GUC_UNIT_S, 60 * 60 * 24},
	{"h", GUC_UNIT_S, 60 * 60},
	{"min", GUC_UNIT_S, 60},
	{"s", GUC_UNIT_S, 1},
	{"ms", GUC_UNIT_S, 1.0 / 1000},
	{"us", GUC_UNIT_S, 1.0 / (1000 * 1000)},

	{"d", GUC_UNIT_MIN, 60 * 24},
	{"h", GUC_UNIT_MIN, 60},
	{"min", GUC_UNIT_MIN, 1},
	{"s", GUC_UNIT_MIN, 1.0 / 60},
	{"ms", GUC_UNIT_MIN, 1.0 / (1000 * 60)},
	{"us", GUC_UNIT_MIN, 1.0 / (1000 * 1000 * 60)},

	{""}						/* end of table marker */
};

/*
 * Contents of GUC tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *	  and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *	  details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *	  applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *	  appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GUC_LIST_QUOTE option, you must add it to
 *	  variable_is_guc_list_quote() in src/bin/pg_dump/dumputils.c.
 */


/******** option records follow ********/










/******** end of options list ********/


/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name
 * should be mapped to a new one only if the new variable has very similar
 * semantics to the old.
 */
static const char *const map_old_guc_names[] = {
	"sort_mem", "work_mem",
	"vacuum_mem", "maintenance_work_mem",
	NULL
};


/*
 * Actual lookup of variables is done through this single, sorted array.
 */
static __thread struct config_generic **guc_variables;

/* Current number of variables contained in the vector */
static __thread int	num_guc_variables;

/* Vector capacity */
static __thread int	size_guc_variables;


static __thread bool guc_dirty;			/* true if need to do commit/abort work */

static __thread bool reporting_enabled;	/* true to enable GUC_REPORT */

static __thread bool report_needed;		/* true if any GUC_REPORT reports are needed */

static __thread int	GUCNestLevel = 0;	/* 1 when in main transaction */


static int	guc_var_compare(const void *a, const void *b);
static int	guc_name_compare(const char *namea, const char *nameb);
static void InitializeGUCOptionsFromEnvironment(void);
static void InitializeOneGUCOption(struct config_generic *gconf);
static void push_old_value(struct config_generic *gconf, GucAction action);
static void ReportGUCOption(struct config_generic *record);
static void reapply_stacked_values(struct config_generic *variable,
								   struct config_string *pHolder,
								   GucStack *stack,
								   const char *curvalue,
								   GucContext curscontext, GucSource cursource);
static void ShowGUCConfigOption(const char *name, DestReceiver *dest);
static void ShowAllGUCConfig(DestReceiver *dest);
static char *_ShowOption(struct config_generic *record, bool use_units);
static bool validate_option_array_item(const char *name, const char *value,
									   bool skipIfNoPermissions);
static void write_auto_conf_file(int fd, const char *filename, ConfigVariable *head_p);
static void replace_auto_config_value(ConfigVariable **head_p, ConfigVariable **tail_p,
									  const char *name, const char *value);


/*
 * Some infrastructure for checking malloc/strdup/realloc calls
 */
static void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	data = malloc(size);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

static void *
guc_realloc(int elevel, void *old, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of realloc(NULL, 0) */
	if (old == NULL && size == 0)
		size = 1;
	data = realloc(old, size);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

static char *
guc_strdup(int elevel, const char *src)
{
	char	   *data;

	data = strdup(src);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}


/*
 * Detect whether strval is referenced anywhere in a GUC string item
 */
static bool
string_field_used(struct config_string *conf, char *strval)
{
	GucStack   *stack;

	if (strval == *(conf->variable) ||
		strval == conf->reset_val ||
		strval == conf->boot_val)
		return true;
	for (stack = conf->gen.stack; stack; stack = stack->prev)
	{
		if (strval == stack->prior.val.stringval ||
			strval == stack->masked.val.stringval)
			return true;
	}
	return false;
}

/*
 * Support for assigning to a field of a string GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_string_field(struct config_string *conf, char **field, char *newval)
{
	char	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/* Free old value if it's not NULL and isn't referenced anymore */
	if (oldval && !string_field_used(conf, oldval))
		free(oldval);
}

/*
 * Detect whether an "extra" struct is referenced anywhere in a GUC item
 */
static bool
extra_field_used(struct config_generic *gconf, void *extra)
{
	GucStack   *stack;

	if (extra == gconf->extra)
		return true;
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			if (extra == ((struct config_bool *) gconf)->reset_extra)
				return true;
			break;
		case PGC_INT:
			if (extra == ((struct config_int *) gconf)->reset_extra)
				return true;
			break;
		case PGC_REAL:
			if (extra == ((struct config_real *) gconf)->reset_extra)
				return true;
			break;
		case PGC_STRING:
			if (extra == ((struct config_string *) gconf)->reset_extra)
				return true;
			break;
		case PGC_ENUM:
			if (extra == ((struct config_enum *) gconf)->reset_extra)
				return true;
			break;
	}
	for (stack = gconf->stack; stack; stack = stack->prev)
	{
		if (extra == stack->prior.extra ||
			extra == stack->masked.extra)
			return true;
	}

	return false;
}

/*
 * Support for assigning to an "extra" field of a GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_extra_field(struct config_generic *gconf, void **field, void *newval)
{
	void	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/* Free old value if it's not NULL and isn't referenced anymore */
	if (oldval && !extra_field_used(gconf, oldval))
		free(oldval);
}

/*
 * Support for copying a variable's active value into a stack entry.
 * The "extra" field associated with the active value is copied, too.
 *
 * NB: be sure stringval and extra fields of a new stack entry are
 * initialized to NULL before this is used, else we'll try to free() them.
 */
static void
set_stack_value(struct config_generic *gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			val->val.boolval =
				*((struct config_bool *) gconf)->variable;
			break;
		case PGC_INT:
			val->val.intval =
				*((struct config_int *) gconf)->variable;
			break;
		case PGC_REAL:
			val->val.realval =
				*((struct config_real *) gconf)->variable;
			break;
		case PGC_STRING:
			set_string_field((struct config_string *) gconf,
							 &(val->val.stringval),
							 *((struct config_string *) gconf)->variable);
			break;
		case PGC_ENUM:
			val->val.enumval =
				*((struct config_enum *) gconf)->variable;
			break;
	}
	set_extra_field(gconf, &(val->extra), gconf->extra);
}

/*
 * Support for discarding a no-longer-needed value in a stack entry.
 * The "extra" field associated with the stack entry is cleared, too.
 */
static void
discard_stack_value(struct config_generic *gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case PGC_BOOL:
		case PGC_INT:
		case PGC_REAL:
		case PGC_ENUM:
			/* no need to do anything */
			break;
		case PGC_STRING:
			set_string_field((struct config_string *) gconf,
							 &(val->val.stringval),
							 NULL);
			break;
	}
	set_extra_field(gconf, &(val->extra), NULL);
}


/*
 * Fetch the sorted array pointer (exported for help_config.c's use ONLY)
 */
struct config_generic **
get_guc_variables(void)
{
	return guc_variables;
}


/*
 * Build the sorted array.  This is split out so that it could be
 * re-executed after startup (e.g., we could allow loadable modules to
 * add vars, and then we'd need to re-sort).
 */
void
build_guc_variables(void)
{
}

/*
 * Add a new GUC variable to the list of known variables. The
 * list is expanded if needed.
 */
static bool
add_guc_variable(struct config_generic *var, int elevel)
{
	if (num_guc_variables + 1 >= size_guc_variables)
	{
		/*
		 * Increase the vector by 25%
		 */
		int			size_vars = size_guc_variables + size_guc_variables / 4;
		struct config_generic **guc_vars;

		if (size_vars == 0)
		{
			size_vars = 100;
			guc_vars = (struct config_generic **)
				guc_malloc(elevel, size_vars * sizeof(struct config_generic *));
		}
		else
		{
			guc_vars = (struct config_generic **)
				guc_realloc(elevel, guc_variables, size_vars * sizeof(struct config_generic *));
		}

		if (guc_vars == NULL)
			return false;		/* out of memory */

		guc_variables = guc_vars;
		size_guc_variables = size_vars;
	}
	guc_variables[num_guc_variables++] = var;
	qsort((void *) guc_variables, num_guc_variables,
		  sizeof(struct config_generic *), guc_var_compare);
	return true;
}

/*
 * Decide whether a proposed custom variable name is allowed.
 *
 * It must be two or more identifiers separated by dots, where the rules
 * for what is an identifier agree with scan.l.  (If you change this rule,
 * adjust the errdetail in find_option().)
 */
static bool
valid_custom_variable_name(const char *name)
{
	bool		saw_sep = false;
	bool		name_start = true;

	for (const char *p = name; *p; p++)
	{
		if (*p == GUC_QUALIFIER_SEPARATOR)
		{
			if (name_start)
				return false;	/* empty name component */
			saw_sep = true;
			name_start = true;
		}
		else if (strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
						"abcdefghijklmnopqrstuvwxyz", *p) != NULL ||
				 IS_HIGHBIT_SET(*p))
		{
			/* okay as first or non-first character */
			name_start = false;
		}
		else if (!name_start && strchr("0123456789_$", *p) != NULL)
			 /* okay as non-first character */ ;
		else
			return false;
	}
	if (name_start)
		return false;			/* empty name component */
	/* OK if we found at least one separator */
	return saw_sep;
}

/*
 * Create and add a placeholder variable for a custom variable name.
 */
static struct config_generic *
add_placeholder_variable(const char *name, int elevel)
{
	size_t		sz = sizeof(struct config_string) + sizeof(char *);
	struct config_string *var;
	struct config_generic *gen;

	var = (struct config_string *) guc_malloc(elevel, sz);
	if (var == NULL)
		return NULL;
	memset(var, 0, sz);
	gen = &var->gen;

	gen->name = guc_strdup(elevel, name);
	if (gen->name == NULL)
	{
		free(var);
		return NULL;
	}

	gen->context = PGC_USERSET;
	gen->group = CUSTOM_OPTIONS;
	gen->short_desc = "GUC placeholder variable";
	gen->flags = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER;
	gen->vartype = PGC_STRING;

	/*
	 * The char* is allocated at the end of the struct since we have no
	 * 'static' place to point to.  Note that the current value, as well as
	 * the boot and reset values, start out NULL.
	 */
	var->variable = (char **) (var + 1);

	if (!add_guc_variable((struct config_generic *) var, elevel))
	{
		free(unconstify(char *, gen->name));
		free(var);
		return NULL;
	}

	return gen;
}

/*
 * Look up option "name".  If it exists, return a pointer to its record.
 * Otherwise, if create_placeholders is true and name is a valid-looking
 * custom variable name, we'll create and return a placeholder record.
 * Otherwise, if skip_errors is true, then we silently return NULL for
 * an unrecognized or invalid name.  Otherwise, the error is reported at
 * error level elevel (and we return NULL if that's less than ERROR).
 *
 * Note: internal errors, primarily out-of-memory, draw an elevel-level
 * report and NULL return regardless of skip_errors.  Hence, callers must
 * handle a NULL return whenever elevel < ERROR, but they should not need
 * to emit any additional error message.  (In practice, internal errors
 * can only happen when create_placeholders is true, so callers passing
 * false need not think terribly hard about this.)
 */
static struct config_generic *
find_option(const char *name, bool create_placeholders, bool skip_errors,
			int elevel)
{
	const char **key = &name;
	struct config_generic **res;
	int			i;

	Assert(name);

	/*
	 * By equating const char ** with struct config_generic *, we are assuming
	 * the name field is first in config_generic.
	 */
	res = (struct config_generic **) bsearch((void *) &key,
											 (void *) guc_variables,
											 num_guc_variables,
											 sizeof(struct config_generic *),
											 guc_var_compare);
	if (res)
		return *res;

	/*
	 * See if the name is an obsolete name for a variable.  We assume that the
	 * set of supported old names is short enough that a brute-force search is
	 * the best way.
	 */
	for (i = 0; map_old_guc_names[i] != NULL; i += 2)
	{
		if (guc_name_compare(name, map_old_guc_names[i]) == 0)
			return find_option(map_old_guc_names[i + 1], false,
							   skip_errors, elevel);
	}

	if (create_placeholders)
	{
		/*
		 * Check if the name is valid, and if so, add a placeholder.  If it
		 * doesn't contain a separator, don't assume that it was meant to be a
		 * placeholder.
		 */
		if (strchr(name, GUC_QUALIFIER_SEPARATOR) != NULL)
		{
			if (valid_custom_variable_name(name))
				return add_placeholder_variable(name, elevel);
			/* A special error message seems desirable here */
			if (!skip_errors)
				ereport(elevel,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("invalid configuration parameter name \"%s\"",
								name),
						 errdetail("Custom parameter names must be two or more simple identifiers separated by dots.")));
			return NULL;
		}
	}

	/* Unknown name */
	if (!skip_errors)
		ereport(elevel,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unrecognized configuration parameter \"%s\"",
						name)));
	return NULL;
}


/*
 * comparator for qsorting and bsearching guc_variables array
 */
static int
guc_var_compare(const void *a, const void *b)
{
	const struct config_generic *confa = *(struct config_generic *const *) a;
	const struct config_generic *confb = *(struct config_generic *const *) b;

	return guc_name_compare(confa->name, confb->name);
}

/*
 * the bare comparison function for GUC names
 */
static int
guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}


/*
 * Initialize GUC options during program startup.
 *
 * Note that we cannot read the config file yet, since we have not yet
 * processed command-line switches.
 */
void
InitializeGUCOptions(void)
{
	int			i;

	/*
	 * Before log_line_prefix could possibly receive a nonempty setting, make
	 * sure that timezone processing is minimally alive (see elog.c).
	 */
	pg_timezone_initialize();

	/*
	 * Build sorted array of all GUC variables.
	 */
	build_guc_variables();

	/*
	 * Load all variables with their compiled-in defaults, and initialize
	 * status fields as needed.
	 */
	for (i = 0; i < num_guc_variables; i++)
	{
		InitializeOneGUCOption(guc_variables[i]);
	}

	guc_dirty = false;

	reporting_enabled = false;

	/*
	 * Prevent any attempt to override the transaction modes from
	 * non-interactive sources.
	 */
	SetConfigOption("transaction_isolation", "read committed",
					PGC_POSTMASTER, PGC_S_OVERRIDE);
	SetConfigOption("transaction_read_only", "no",
					PGC_POSTMASTER, PGC_S_OVERRIDE);
	SetConfigOption("transaction_deferrable", "no",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	/*
	 * For historical reasons, some GUC parameters can receive defaults from
	 * environment variables.  Process those settings.
	 */
	InitializeGUCOptionsFromEnvironment();
}

/*
 * Assign any GUC values that can come from the server's environment.
 *
 * This is called from InitializeGUCOptions, and also from ProcessConfigFile
 * to deal with the possibility that a setting has been removed from
 * postgresql.conf and should now get a value from the environment.
 * (The latter is a kludge that should probably go away someday; if so,
 * fold this back into InitializeGUCOptions.)
 */
static void
InitializeGUCOptionsFromEnvironment(void)
{
	char	   *env;
	long		stack_rlimit;

	env = getenv("PGPORT");
	if (env != NULL)
		SetConfigOption("port", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	env = getenv("PGDATESTYLE");
	if (env != NULL)
		SetConfigOption("datestyle", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	env = getenv("PGCLIENTENCODING");
	if (env != NULL)
		SetConfigOption("client_encoding", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	/*
	 * rlimit isn't exactly an "environment variable", but it behaves about
	 * the same.  If we can identify the platform stack depth rlimit, increase
	 * default stack depth setting up to whatever is safe (but at most 2MB).
	 */
	stack_rlimit = get_stack_depth_rlimit();
	if (stack_rlimit > 0)
	{
		long		new_limit = (stack_rlimit - STACK_DEPTH_SLOP) / 1024L;

		if (new_limit > 100)
		{
			char		limbuf[16];

			new_limit = Min(new_limit, 2048);
			sprintf(limbuf, "%ld", new_limit);
			SetConfigOption("max_stack_depth", limbuf,
							PGC_POSTMASTER, PGC_S_ENV_VAR);
		}
	}
}

/*
 * Initialize one GUC option variable to its compiled-in default.
 *
 * Note: the reason for calling check_hooks is not that we think the boot_val
 * might fail, but that the hooks might wish to compute an "extra" struct.
 */
static void
InitializeOneGUCOption(struct config_generic *gconf)
{
	gconf->status = 0;
	gconf->source = PGC_S_DEFAULT;
	gconf->reset_source = PGC_S_DEFAULT;
	gconf->scontext = PGC_INTERNAL;
	gconf->reset_scontext = PGC_INTERNAL;
	gconf->stack = NULL;
	gconf->extra = NULL;
	gconf->last_reported = NULL;
	gconf->sourcefile = NULL;
	gconf->sourceline = 0;

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) gconf;
				bool		newval = conf->boot_val;
				void	   *extra = NULL;

				if (!call_bool_check_hook(conf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 conf->gen.name, (int) newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;
				int			newval = conf->boot_val;
				void	   *extra = NULL;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_int_check_hook(conf, &newval, &extra,
										 PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 conf->gen.name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) gconf;
				double		newval = conf->boot_val;
				void	   *extra = NULL;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_real_check_hook(conf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %g",
						 conf->gen.name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;
				char	   *newval;
				void	   *extra = NULL;

				/* non-NULL boot_val must always get strdup'd */
				if (conf->boot_val != NULL)
					newval = guc_strdup(FATAL, conf->boot_val);
				else
					newval = NULL;

				if (!call_string_check_hook(conf, &newval, &extra,
											PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to \"%s\"",
						 conf->gen.name, newval ? newval : "");
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;
				int			newval = conf->boot_val;
				void	   *extra = NULL;

				if (!call_enum_check_hook(conf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 conf->gen.name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
	}
}


/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success; on failure, prints a suitable error message
 * to stderr and returns false.
 */
bool
SelectConfigFiles(const char *userDoption, const char *progname)
{
	char	   *configdir;
	char	   *fname;
	struct stat stat_buf;

	/* configdir is -D option, or $PGDATA if no -D */
	if (userDoption)
		configdir = make_absolute_path(userDoption);
	else
		configdir = make_absolute_path(getenv("PGDATA"));

	if (configdir && stat(configdir, &stat_buf) != 0)
	{
		write_stderr("%s: could not access directory \"%s\": %s\n",
					 progname,
					 configdir,
					 strerror(errno));
		if (errno == ENOENT)
			write_stderr("Run initdb or pg_basebackup to initialize a PostgreSQL data directory.\n");
		return false;
	}

	/*
	 * Find the configuration file: if config_file was specified on the
	 * command line, use it, else use configdir/postgresql.conf.  In any case
	 * ensure the result is an absolute path, so that it will be interpreted
	 * the same way by future backends.
	 */
	if (ConfigFileName)
		fname = make_absolute_path(ConfigFileName);
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(CONFIG_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, CONFIG_FILENAME);
	}
	else
	{
		write_stderr("%s does not know where to find the server configuration file.\n"
					 "You must specify the --config-file or -D invocation "
					 "option or set the PGDATA environment variable.\n",
					 progname);
		return false;
	}

	/*
	 * Set the ConfigFileName GUC variable to its final value, ensuring that
	 * it can't be overridden later.
	 */
	SetConfigOption("config_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
	free(fname);

	/*
	 * Now read the config file for the first time.
	 */
	if (stat(ConfigFileName, &stat_buf) != 0)
	{
		write_stderr("%s: could not access the server configuration file \"%s\": %s\n",
					 progname, ConfigFileName, strerror(errno));
		free(configdir);
		return false;
	}

	/*
	 * Read the configuration file for the first time.  This time only the
	 * data_directory parameter is picked up to determine the data directory,
	 * so that we can read the PG_AUTOCONF_FILENAME file next time.
	 */
	ProcessConfigFile(PGC_POSTMASTER);

	/*
	 * If the data_directory GUC variable has been set, use that as DataDir;
	 * otherwise use configdir if set; else punt.
	 *
	 * Note: SetDataDir will copy and absolute-ize its argument, so we don't
	 * have to.
	 */
	if (data_directory)
		SetDataDir(data_directory);
	else if (configdir)
		SetDataDir(configdir);
	else
	{
		write_stderr("%s does not know where to find the database system data.\n"
					 "This can be specified as \"data_directory\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		return false;
	}

	/*
	 * Reflect the final DataDir value back into the data_directory GUC var.
	 * (If you are wondering why we don't just make them a single variable,
	 * it's because the EXEC_BACKEND case needs DataDir to be transmitted to
	 * child backends specially.  XXX is that still true?  Given that we now
	 * chdir to DataDir, EXEC_BACKEND can read the config file without knowing
	 * DataDir in advance.)
	 */
	SetConfigOption("data_directory", DataDir, PGC_POSTMASTER, PGC_S_OVERRIDE);

	/*
	 * Now read the config file a second time, allowing any settings in the
	 * PG_AUTOCONF_FILENAME file to take effect.  (This is pretty ugly, but
	 * since we have to determine the DataDir before we can find the autoconf
	 * file, the alternatives seem worse.)
	 */
	ProcessConfigFile(PGC_POSTMASTER);

	/*
	 * If timezone_abbreviations wasn't set in the configuration file, install
	 * the default value.  We do it this way because we can't safely install a
	 * "real" value until my_exec_path is set, which may not have happened
	 * when InitializeGUCOptions runs, so the bootstrap default value cannot
	 * be the real desired default.
	 */
	pg_timezone_abbrev_initialize();

	/*
	 * Figure out where pg_hba.conf is, and make sure the path is absolute.
	 */
	if (HbaFileName)
		fname = make_absolute_path(HbaFileName);
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(HBA_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, HBA_FILENAME);
	}
	else
	{
		write_stderr("%s does not know where to find the \"hba\" configuration file.\n"
					 "This can be specified as \"hba_file\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		return false;
	}
	SetConfigOption("hba_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
	free(fname);

	/*
	 * Likewise for pg_ident.conf.
	 */
	if (IdentFileName)
		fname = make_absolute_path(IdentFileName);
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(IDENT_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, IDENT_FILENAME);
	}
	else
	{
		write_stderr("%s does not know where to find the \"ident\" configuration file.\n"
					 "This can be specified as \"ident_file\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		return false;
	}
	SetConfigOption("ident_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
	free(fname);

	free(configdir);

	return true;
}


/*
 * Reset all options to their saved default values (implements RESET ALL)
 */
void
ResetAllOptions(void)
{
	int			i;

	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = guc_variables[i];

		/* Don't reset non-SET-able values */
		if (gconf->context != PGC_SUSET &&
			gconf->context != PGC_USERSET)
			continue;
		/* Don't reset if special exclusion from RESET ALL */
		if (gconf->flags & GUC_NO_RESET_ALL)
			continue;
		/* No need to reset if wasn't SET */
		if (gconf->source <= PGC_S_OVERRIDE)
			continue;

		/* Save old value to support transaction abort */
		push_old_value(gconf, GUC_ACTION_SET);

		switch (gconf->vartype)
		{
			case PGC_BOOL:
				{
					struct config_bool *conf = (struct config_bool *) gconf;

					if (conf->assign_hook)
						conf->assign_hook(conf->reset_val,
										  conf->reset_extra);
					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case PGC_INT:
				{
					struct config_int *conf = (struct config_int *) gconf;

					if (conf->assign_hook)
						conf->assign_hook(conf->reset_val,
										  conf->reset_extra);
					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case PGC_REAL:
				{
					struct config_real *conf = (struct config_real *) gconf;

					if (conf->assign_hook)
						conf->assign_hook(conf->reset_val,
										  conf->reset_extra);
					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case PGC_STRING:
				{
					struct config_string *conf = (struct config_string *) gconf;

					if (conf->assign_hook)
						conf->assign_hook(conf->reset_val,
										  conf->reset_extra);
					set_string_field(conf, conf->variable, conf->reset_val);
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case PGC_ENUM:
				{
					struct config_enum *conf = (struct config_enum *) gconf;

					if (conf->assign_hook)
						conf->assign_hook(conf->reset_val,
										  conf->reset_extra);
					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
		}

		gconf->source = gconf->reset_source;
		gconf->scontext = gconf->reset_scontext;

		if (gconf->flags & GUC_REPORT)
		{
			gconf->status |= GUC_NEEDS_REPORT;
			report_needed = true;
		}
	}
}


/*
 * push_old_value
 *		Push previous state during transactional assignment to a GUC variable.
 */
static void
push_old_value(struct config_generic *gconf, GucAction action)
{
	GucStack   *stack;

	/* If we're not inside a nest level, do nothing */
	if (GUCNestLevel == 0)
		return;

	/* Do we already have a stack entry of the current nest level? */
	stack = gconf->stack;
	if (stack && stack->nest_level >= GUCNestLevel)
	{
		/* Yes, so adjust its state if necessary */
		Assert(stack->nest_level == GUCNestLevel);
		switch (action)
		{
			case GUC_ACTION_SET:
				/* SET overrides any prior action at same nest level */
				if (stack->state == GUC_SET_LOCAL)
				{
					/* must discard old masked value */
					discard_stack_value(gconf, &stack->masked);
				}
				stack->state = GUC_SET;
				break;
			case GUC_ACTION_LOCAL:
				if (stack->state == GUC_SET)
				{
					/* SET followed by SET LOCAL, remember SET's value */
					stack->masked_scontext = gconf->scontext;
					set_stack_value(gconf, &stack->masked);
					stack->state = GUC_SET_LOCAL;
				}
				/* in all other cases, no change to stack entry */
				break;
			case GUC_ACTION_SAVE:
				/* Could only have a prior SAVE of same variable */
				Assert(stack->state == GUC_SAVE);
				break;
		}
		Assert(guc_dirty);		/* must be set already */
		return;
	}

	/*
	 * Push a new stack entry
	 *
	 * We keep all the stack entries in TopTransactionContext for simplicity.
	 */
	stack = (GucStack *) MemoryContextAllocZero(TopTransactionContext,
												sizeof(GucStack));

	stack->prev = gconf->stack;
	stack->nest_level = GUCNestLevel;
	switch (action)
	{
		case GUC_ACTION_SET:
			stack->state = GUC_SET;
			break;
		case GUC_ACTION_LOCAL:
			stack->state = GUC_LOCAL;
			break;
		case GUC_ACTION_SAVE:
			stack->state = GUC_SAVE;
			break;
	}
	stack->source = gconf->source;
	stack->scontext = gconf->scontext;
	set_stack_value(gconf, &stack->prior);

	gconf->stack = stack;

	/* Ensure we remember to pop at end of xact */
	guc_dirty = true;
}


/*
 * Do GUC processing at main transaction start.
 */
void
AtStart_GUC(void)
{
	/*
	 * The nest level should be 0 between transactions; if it isn't, somebody
	 * didn't call AtEOXact_GUC, or called it with the wrong nestLevel.  We
	 * throw a warning but make no other effort to clean up.
	 */
	if (GUCNestLevel != 0)
		elog(WARNING, "GUC nest level = %d at transaction start",
			 GUCNestLevel);
	GUCNestLevel = 1;
}

/*
 * Enter a new nesting level for GUC values.  This is called at subtransaction
 * start, and when entering a function that has proconfig settings, and in
 * some other places where we want to set GUC variables transiently.
 * NOTE we must not risk error here, else subtransaction start will be unhappy.
 */
int
NewGUCNestLevel(void)
{
	return ++GUCNestLevel;
}

/*
 * Do GUC processing at transaction or subtransaction commit or abort, or
 * when exiting a function that has proconfig settings, or when undoing a
 * transient assignment to some GUC variables.  (The name is thus a bit of
 * a misnomer; perhaps it should be ExitGUCNestLevel or some such.)
 * During abort, we discard all GUC settings that were applied at nesting
 * levels >= nestLevel.  nestLevel == 1 corresponds to the main transaction.
 */
void
AtEOXact_GUC(bool isCommit, int nestLevel)
{
	bool		still_dirty;
	int			i;

	/*
	 * Note: it's possible to get here with GUCNestLevel == nestLevel-1 during
	 * abort, if there is a failure during transaction start before
	 * AtStart_GUC is called.
	 */
	Assert(nestLevel > 0 &&
		   (nestLevel <= GUCNestLevel ||
			(nestLevel == GUCNestLevel + 1 && !isCommit)));

	/* Quick exit if nothing's changed in this transaction */
	if (!guc_dirty)
	{
		GUCNestLevel = nestLevel - 1;
		return;
	}

	still_dirty = false;
	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = guc_variables[i];
		GucStack   *stack;

		/*
		 * Process and pop each stack entry within the nest level. To simplify
		 * fmgr_security_definer() and other places that use GUC_ACTION_SAVE,
		 * we allow failure exit from code that uses a local nest level to be
		 * recovered at the surrounding transaction or subtransaction abort;
		 * so there could be more than one stack entry to pop.
		 */
		while ((stack = gconf->stack) != NULL &&
			   stack->nest_level >= nestLevel)
		{
			GucStack   *prev = stack->prev;
			bool		restorePrior = false;
			bool		restoreMasked = false;
			bool		changed;

			/*
			 * In this next bit, if we don't set either restorePrior or
			 * restoreMasked, we must "discard" any unwanted fields of the
			 * stack entries to avoid leaking memory.  If we do set one of
			 * those flags, unused fields will be cleaned up after restoring.
			 */
			if (!isCommit)		/* if abort, always restore prior value */
				restorePrior = true;
			else if (stack->state == GUC_SAVE)
				restorePrior = true;
			else if (stack->nest_level == 1)
			{
				/* transaction commit */
				if (stack->state == GUC_SET_LOCAL)
					restoreMasked = true;
				else if (stack->state == GUC_SET)
				{
					/* we keep the current active value */
					discard_stack_value(gconf, &stack->prior);
				}
				else			/* must be GUC_LOCAL */
					restorePrior = true;
			}
			else if (prev == NULL ||
					 prev->nest_level < stack->nest_level - 1)
			{
				/* decrement entry's level and do not pop it */
				stack->nest_level--;
				continue;
			}
			else
			{
				/*
				 * We have to merge this stack entry into prev. See README for
				 * discussion of this bit.
				 */
				switch (stack->state)
				{
					case GUC_SAVE:
						Assert(false);	/* can't get here */
						break;

					case GUC_SET:
						/* next level always becomes SET */
						discard_stack_value(gconf, &stack->prior);
						if (prev->state == GUC_SET_LOCAL)
							discard_stack_value(gconf, &prev->masked);
						prev->state = GUC_SET;
						break;

					case GUC_LOCAL:
						if (prev->state == GUC_SET)
						{
							/* LOCAL migrates down */
							prev->masked_scontext = stack->scontext;
							prev->masked = stack->prior;
							prev->state = GUC_SET_LOCAL;
						}
						else
						{
							/* else just forget this stack level */
							discard_stack_value(gconf, &stack->prior);
						}
						break;

					case GUC_SET_LOCAL:
						/* prior state at this level no longer wanted */
						discard_stack_value(gconf, &stack->prior);
						/* copy down the masked state */
						prev->masked_scontext = stack->masked_scontext;
						if (prev->state == GUC_SET_LOCAL)
							discard_stack_value(gconf, &prev->masked);
						prev->masked = stack->masked;
						prev->state = GUC_SET_LOCAL;
						break;
				}
			}

			changed = false;

			if (restorePrior || restoreMasked)
			{
				/* Perform appropriate restoration of the stacked value */
				config_var_value newvalue;
				GucSource	newsource;
				GucContext	newscontext;

				if (restoreMasked)
				{
					newvalue = stack->masked;
					newsource = PGC_S_SESSION;
					newscontext = stack->masked_scontext;
				}
				else
				{
					newvalue = stack->prior;
					newsource = stack->source;
					newscontext = stack->scontext;
				}

				switch (gconf->vartype)
				{
					case PGC_BOOL:
						{
							struct config_bool *conf = (struct config_bool *) gconf;
							bool		newval = newvalue.val.boolval;
							void	   *newextra = newvalue.extra;

							if (*conf->variable != newval ||
								conf->gen.extra != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*conf->variable = newval;
								set_extra_field(&conf->gen, &conf->gen.extra,
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_INT:
						{
							struct config_int *conf = (struct config_int *) gconf;
							int			newval = newvalue.val.intval;
							void	   *newextra = newvalue.extra;

							if (*conf->variable != newval ||
								conf->gen.extra != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*conf->variable = newval;
								set_extra_field(&conf->gen, &conf->gen.extra,
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_REAL:
						{
							struct config_real *conf = (struct config_real *) gconf;
							double		newval = newvalue.val.realval;
							void	   *newextra = newvalue.extra;

							if (*conf->variable != newval ||
								conf->gen.extra != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*conf->variable = newval;
								set_extra_field(&conf->gen, &conf->gen.extra,
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_STRING:
						{
							struct config_string *conf = (struct config_string *) gconf;
							char	   *newval = newvalue.val.stringval;
							void	   *newextra = newvalue.extra;

							if (*conf->variable != newval ||
								conf->gen.extra != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								set_string_field(conf, conf->variable, newval);
								set_extra_field(&conf->gen, &conf->gen.extra,
												newextra);
								changed = true;
							}

							/*
							 * Release stacked values if not used anymore. We
							 * could use discard_stack_value() here, but since
							 * we have type-specific code anyway, might as
							 * well inline it.
							 */
							set_string_field(conf, &stack->prior.val.stringval, NULL);
							set_string_field(conf, &stack->masked.val.stringval, NULL);
							break;
						}
					case PGC_ENUM:
						{
							struct config_enum *conf = (struct config_enum *) gconf;
							int			newval = newvalue.val.enumval;
							void	   *newextra = newvalue.extra;

							if (*conf->variable != newval ||
								conf->gen.extra != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*conf->variable = newval;
								set_extra_field(&conf->gen, &conf->gen.extra,
												newextra);
								changed = true;
							}
							break;
						}
				}

				/*
				 * Release stacked extra values if not used anymore.
				 */
				set_extra_field(gconf, &(stack->prior.extra), NULL);
				set_extra_field(gconf, &(stack->masked.extra), NULL);

				/* And restore source information */
				gconf->source = newsource;
				gconf->scontext = newscontext;
			}

			/* Finish popping the state stack */
			gconf->stack = prev;
			pfree(stack);

			/* Report new value if we changed it */
			if (changed && (gconf->flags & GUC_REPORT))
			{
				gconf->status |= GUC_NEEDS_REPORT;
				report_needed = true;
			}
		}						/* end of stack-popping loop */

		if (stack != NULL)
			still_dirty = true;
	}

	/* If there are no remaining stack entries, we can reset guc_dirty */
	guc_dirty = still_dirty;

	/* Update nesting level */
	GUCNestLevel = nestLevel - 1;
}


/*
 * Start up automatic reporting of changes to variables marked GUC_REPORT.
 * This is executed at completion of backend startup.
 */
void
BeginReportingGUCOptions(void)
{
	int			i;

	/*
	 * Don't do anything unless talking to an interactive frontend.
	 */
	if (whereToSendOutput != DestRemote)
		return;

	reporting_enabled = true;

	/*
	 * Hack for in_hot_standby: initialize with the value we're about to send.
	 * (This could be out of date by the time we actually send it, in which
	 * case the next ReportChangedGUCOptions call will send a duplicate
	 * report.)
	 */
	in_hot_standby = RecoveryInProgress();

	/* Transmit initial values of interesting variables */
	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *conf = guc_variables[i];

		if (conf->flags & GUC_REPORT)
			ReportGUCOption(conf);
	}

	report_needed = false;
}

/*
 * ReportChangedGUCOptions: report recently-changed GUC_REPORT variables
 *
 * This is called just before we wait for a new client query.
 *
 * By handling things this way, we ensure that a ParameterStatus message
 * is sent at most once per variable per query, even if the variable
 * changed multiple times within the query.  That's quite possible when
 * using features such as function SET clauses.  Function SET clauses
 * also tend to cause values to change intraquery but eventually revert
 * to their prevailing values; ReportGUCOption is responsible for avoiding
 * redundant reports in such cases.
 */
void
ReportChangedGUCOptions(void)
{
	/* Quick exit if not (yet) enabled */
	if (!reporting_enabled)
		return;

	/*
	 * Since in_hot_standby isn't actually changed by normal GUC actions, we
	 * need a hack to check whether a new value needs to be reported to the
	 * client.  For speed, we rely on the assumption that it can never
	 * transition from false to true.
	 */
	if (in_hot_standby && !RecoveryInProgress())
	{
		struct config_generic *record;

		record = find_option("in_hot_standby", false, false, ERROR);
		Assert(record != NULL);
		record->status |= GUC_NEEDS_REPORT;
		report_needed = true;
		in_hot_standby = false;
	}

	/* Quick exit if no values have been changed */
	if (!report_needed)
		return;

	/* Transmit new values of interesting variables */
	for (int i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *conf = guc_variables[i];

		if ((conf->flags & GUC_REPORT) && (conf->status & GUC_NEEDS_REPORT))
			ReportGUCOption(conf);
	}

	report_needed = false;
}

/*
 * ReportGUCOption: if appropriate, transmit option value to frontend
 *
 * We need not transmit the value if it's the same as what we last
 * transmitted.  However, clear the NEEDS_REPORT flag in any case.
 */
static void
ReportGUCOption(struct config_generic *record)
{
	char	   *val = _ShowOption(record, false);

	if (record->last_reported == NULL ||
		strcmp(val, record->last_reported) != 0)
	{
		StringInfoData msgbuf;

		pq_beginmessage(&msgbuf, 'S');
		pq_sendstring(&msgbuf, record->name);
		pq_sendstring(&msgbuf, val);
		pq_endmessage(&msgbuf);

		/*
		 * We need a long-lifespan copy.  If strdup() fails due to OOM, we'll
		 * set last_reported to NULL and thereby possibly make a duplicate
		 * report later.
		 */
		if (record->last_reported)
			free(record->last_reported);
		record->last_reported = strdup(val);
	}

	pfree(val);

	record->status &= ~GUC_NEEDS_REPORT;
}

/*
 * Convert a value from one of the human-friendly units ("kB", "min" etc.)
 * to the given base unit.  'value' and 'unit' are the input value and unit
 * to convert from (there can be trailing spaces in the unit string).
 * The converted value is stored in *base_value.
 * It's caller's responsibility to round off the converted value as necessary
 * and check for out-of-range.
 *
 * Returns true on success, false if the input unit is not recognized.
 */
static bool
convert_to_base_unit(double value, const char *unit,
					 int base_unit, double *base_value)
{
	char		unitstr[MAX_UNIT_LEN + 1];
	int			unitlen;
	const unit_conversion *table;
	int			i;

	/* extract unit string to compare to table entries */
	unitlen = 0;
	while (*unit != '\0' && !isspace((unsigned char) *unit) &&
		   unitlen < MAX_UNIT_LEN)
		unitstr[unitlen++] = *(unit++);
	unitstr[unitlen] = '\0';
	/* allow whitespace after unit */
	while (isspace((unsigned char) *unit))
		unit++;
	if (*unit != '\0')
		return false;			/* unit too long, or garbage after it */

	/* now search the appropriate table */
	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit &&
			strcmp(unitstr, table[i].unit) == 0)
		{
			double		cvalue = value * table[i].multiplier;

			/*
			 * If the user gave a fractional value such as "30.1GB", round it
			 * off to the nearest multiple of the next smaller unit, if there
			 * is one.
			 */
			if (*table[i + 1].unit &&
				base_unit == table[i + 1].base_unit)
				cvalue = rint(cvalue / table[i + 1].multiplier) *
					table[i + 1].multiplier;

			*base_value = cvalue;
			return true;
		}
	}
	return false;
}

/*
 * Convert an integer value in some base unit to a human-friendly unit.
 *
 * The output unit is chosen so that it's the greatest unit that can represent
 * the value without loss.  For example, if the base unit is GUC_UNIT_KB, 1024
 * is converted to 1 MB, but 1025 is represented as 1025 kB.
 */
static void
convert_int_from_base_unit(int64 base_value, int base_unit,
						   int64 *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly.  We
			 * assume that the conversions for each base unit are ordered from
			 * greatest unit to the smallest!
			 */
			if (table[i].multiplier <= 1.0 ||
				base_value % (int64) table[i].multiplier == 0)
			{
				*value = (int64) rint(base_value / table[i].multiplier);
				*unit = table[i].unit;
				break;
			}
		}
	}

	Assert(*unit != NULL);
}

/*
 * Convert a floating-point value in some base unit to a human-friendly unit.
 *
 * Same as above, except we have to do the math a bit differently, and
 * there's a possibility that we don't find any exact divisor.
 */
static void
convert_real_from_base_unit(double base_value, int base_unit,
							double *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly; or
			 * if there is none, use the smallest (last) target unit.
			 *
			 * What we actually care about here is whether snprintf with "%g"
			 * will print the value as an integer, so the obvious test of
			 * "*value == rint(*value)" is too strict; roundoff error might
			 * make us choose an unreasonably small unit.  As a compromise,
			 * accept a divisor that is within 1e-8 of producing an integer.
			 */
			*value = base_value / table[i].multiplier;
			*unit = table[i].unit;
			if (*value > 0 &&
				fabs((rint(*value) / *value) - 1.0) <= 1e-8)
				break;
		}
	}

	Assert(*unit != NULL);
}

/*
 * Return the name of a GUC's base unit (e.g. "ms") given its flags.
 * Return NULL if the GUC is unitless.
 */
static const char *
get_config_unit_name(int flags)
{
	switch (flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME))
	{
		case 0:
			return NULL;		/* GUC has no units */
		case GUC_UNIT_BYTE:
			return "B";
		case GUC_UNIT_KB:
			return "kB";
		case GUC_UNIT_MB:
			return "MB";
		case GUC_UNIT_BLOCKS:
			{
				static __thread char bbuf[8];

				/* initialize if first time through */
				if (bbuf[0] == '\0')
					snprintf(bbuf, sizeof(bbuf), "%dkB", BLCKSZ / 1024);
				return bbuf;
			}
		case GUC_UNIT_XBLOCKS:
			{
				static __thread char xbuf[8];

				/* initialize if first time through */
				if (xbuf[0] == '\0')
					snprintf(xbuf, sizeof(xbuf), "%dkB", XLOG_BLCKSZ / 1024);
				return xbuf;
			}
		case GUC_UNIT_MS:
			return "ms";
		case GUC_UNIT_S:
			return "s";
		case GUC_UNIT_MIN:
			return "min";
		default:
			elog(ERROR, "unrecognized GUC units value: %d",
				 flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME));
			return NULL;
	}
}


/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, as well as floating-point
 * formats (which will be rounded to integer after any units conversion).
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_int(const char *value, int *result, int flags, const char **hintmsg)
{
	/*
	 * We assume here that double is wide enough to represent any integer
	 * value with adequate precision.
	 */
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	/*
	 * Try to parse as an integer (allowing octal or hex input).  If the
	 * conversion stops at a decimal point or 'e', or overflows, re-parse as
	 * float.  This should work fine as long as we have no unit names starting
	 * with 'e'.  If we ever do, the test could be extended to check for a
	 * sign or digit after 'e', but for now that's unnecessary.
	 */
	errno = 0;
	val = strtol(value, &endptr, 0);
	if (*endptr == '.' || *endptr == 'e' || *endptr == 'E' ||
		errno == ERANGE)
	{
		errno = 0;
		val = strtod(value, &endptr);
	}

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range check below) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	/* Round to int, then check for overflow */
	val = rint(val);

	if (val > INT_MAX || val < INT_MIN)
	{
		if (hintmsg)
			*hintmsg = gettext_noop("Value exceeds integer range.");
		return false;
	}

	if (result)
		*result = (int) val;
	return true;
}

/*
 * Try to parse value as a floating point number in the usual format.
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_real(const char *value, double *result, int flags, const char **hintmsg)
{
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	errno = 0;
	val = strtod(value, &endptr);

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range checks later) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	if (result)
		*result = val;
	return true;
}


/*
 * Lookup the name for an enum option with the selected value.
 * Should only ever be called with known-valid values, so throws
 * an elog(ERROR) if the enum option is not found.
 *
 * The returned string is a pointer to static data and not
 * allocated for modification.
 */
const char *
config_enum_lookup_by_value(struct config_enum *record, int val)
{
	const struct config_enum_entry *entry;

	for (entry = record->options; entry && entry->name; entry++)
	{
		if (entry->val == val)
			return entry->name;
	}

	elog(ERROR, "could not find enum option %d for %s",
		 val, record->gen.name);
	return NULL;				/* silence compiler */
}


/*
 * Lookup the value for an enum option with the selected name
 * (case-insensitive).
 * If the enum option is found, sets the retval value and returns
 * true. If it's not found, return false and retval is set to 0.
 */
bool
config_enum_lookup_by_name(struct config_enum *record, const char *value,
						   int *retval)
{
	const struct config_enum_entry *entry;

	for (entry = record->options; entry && entry->name; entry++)
	{
		if (pg_strcasecmp(value, entry->name) == 0)
		{
			*retval = entry->val;
			return true;
		}
	}

	*retval = 0;
	return false;
}


/*
 * Return a list of all available options for an enum, excluding
 * hidden ones, separated by the given separator.
 * If prefix is non-NULL, it is added before the first enum value.
 * If suffix is non-NULL, it is added to the end of the string.
 */
static char *
config_enum_get_options(struct config_enum *record, const char *prefix,
						const char *suffix, const char *separator)
{
	const struct config_enum_entry *entry;
	StringInfoData retstr;
	int			seplen;

	initStringInfo(&retstr);
	appendStringInfoString(&retstr, prefix);

	seplen = strlen(separator);
	for (entry = record->options; entry && entry->name; entry++)
	{
		if (!entry->hidden)
		{
			appendStringInfoString(&retstr, entry->name);
			appendBinaryStringInfo(&retstr, separator, seplen);
		}
	}

	/*
	 * All the entries may have been hidden, leaving the string empty if no
	 * prefix was given. This indicates a broken GUC setup, since there is no
	 * use for an enum without any values, so we just check to make sure we
	 * don't write to invalid memory instead of actually trying to do
	 * something smart with it.
	 */
	if (retstr.len >= seplen)
	{
		/* Replace final separator */
		retstr.data[retstr.len - seplen] = '\0';
		retstr.len -= seplen;
	}

	appendStringInfoString(&retstr, suffix);

	return retstr.data;
}

/*
 * Parse and validate a proposed value for the specified configuration
 * parameter.
 *
 * This does built-in checks (such as range limits for an integer parameter)
 * and also calls any check hook the parameter may have.
 *
 * record: GUC variable's info record
 * name: variable name (should match the record of course)
 * value: proposed value, as a string
 * source: identifies source of value (check hooks may need this)
 * elevel: level to log any error reports at
 * newval: on success, converted parameter value is returned here
 * newextra: on success, receives any "extra" data returned by check hook
 *	(caller must initialize *newextra to NULL)
 *
 * Returns true if OK, false if not (or throws error, if elevel >= ERROR)
 */
static bool
parse_and_validate_value(struct config_generic *record,
						 const char *name, const char *value,
						 GucSource source, int elevel,
						 union config_var_val *newval, void **newextra)
{
	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;

				if (!parse_bool(value, &newval->boolval))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("parameter \"%s\" requires a Boolean value",
									name)));
					return false;
				}

				if (!call_bool_check_hook(conf, &newval->boolval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) record;
				const char *hintmsg;

				if (!parse_int(value, &newval->intval,
							   conf->gen.flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->intval < conf->min || newval->intval > conf->max)
				{
					const char *unit = get_config_unit_name(conf->gen.flags);

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%d%s%s is outside the valid range for parameter \"%s\" (%d .. %d)",
									newval->intval,
									unit ? " " : "",
									unit ? unit : "",
									name,
									conf->min, conf->max)));
					return false;
				}

				if (!call_int_check_hook(conf, &newval->intval, newextra,
										 source, elevel))
					return false;
			}
			break;
		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;
				const char *hintmsg;

				if (!parse_real(value, &newval->realval,
								conf->gen.flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->realval < conf->min || newval->realval > conf->max)
				{
					const char *unit = get_config_unit_name(conf->gen.flags);

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%g%s%s is outside the valid range for parameter \"%s\" (%g .. %g)",
									newval->realval,
									unit ? " " : "",
									unit ? unit : "",
									name,
									conf->min, conf->max)));
					return false;
				}

				if (!call_real_check_hook(conf, &newval->realval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

				/*
				 * The value passed by the caller could be transient, so we
				 * always strdup it.
				 */
				newval->stringval = guc_strdup(elevel, value);
				if (newval->stringval == NULL)
					return false;

				/*
				 * The only built-in "parsing" check we have is to apply
				 * truncation if GUC_IS_NAME.
				 */
				if (conf->gen.flags & GUC_IS_NAME)
					truncate_identifier(newval->stringval,
										strlen(newval->stringval),
										true);

				if (!call_string_check_hook(conf, &newval->stringval, newextra,
											source, elevel))
				{
					free(newval->stringval);
					newval->stringval = NULL;
					return false;
				}
			}
			break;
		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;

				if (!config_enum_lookup_by_name(conf, value, &newval->enumval))
				{
					char	   *hintmsg;

					hintmsg = config_enum_get_options(conf,
													  "Available values: ",
													  ".", ", ");

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));

					if (hintmsg)
						pfree(hintmsg);
					return false;
				}

				if (!call_enum_check_hook(conf, &newval->enumval, newextra,
										  source, elevel))
					return false;
			}
			break;
	}

	return true;
}


/*
 * Sets option `name' to given value.
 *
 * The value should be a string, which will be parsed and converted to
 * the appropriate data type.  The context and source parameters indicate
 * in which context this function is being called, so that it can apply the
 * access restrictions properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == PGC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * elevel should normally be passed as zero, allowing this function to make
 * its standard choice of ereport level.  However some callers need to be
 * able to override that choice; they should pass the ereport level to use.
 *
 * is_reload should be true only when called from read_nondefault_variables()
 * or RestoreGUCState(), where we are trying to load some other process's
 * GUC settings into a new process.
 *
 * Return value:
 *	+1: the value is valid and was successfully applied.
 *	0:	the name or value is invalid (but see below).
 *	-1: the value was not applied because of context, priority, or changeVal.
 *
 * If there is an error (non-existing option, invalid value) then an
 * ereport(ERROR) is thrown *unless* this is called for a source for which
 * we don't want an ERROR (currently, those are defaults, the config file,
 * and per-database or per-user settings, as well as callers who specify
 * a less-than-ERROR elevel).  In those cases we write a suitable error
 * message via ereport() and return 0.
 *
 * See also SetConfigOption for an external interface.
 */
int
set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  GucAction action, bool changeVal, int elevel,
				  bool is_reload)
{
	struct config_generic *record;
	union config_var_val newval_union;
	void	   *newextra = NULL;
	bool		prohibitValueChange = false;
	bool		makeDefault;

	if (elevel == 0)
	{
		if (source == PGC_S_DEFAULT || source == PGC_S_FILE)
		{
			/*
			 * To avoid cluttering the log, only the postmaster bleats loudly
			 * about problems with the config file.
			 */
			elevel = IsUnderPostmaster ? DEBUG3 : LOG;
		}
		else if (source == PGC_S_GLOBAL ||
				 source == PGC_S_DATABASE ||
				 source == PGC_S_USER ||
				 source == PGC_S_DATABASE_USER)
			elevel = WARNING;
		else
			elevel = ERROR;
	}

	/*
	 * GUC_ACTION_SAVE changes are acceptable during a parallel operation,
	 * because the current worker will also pop the change.  We're probably
	 * dealing with a function having a proconfig entry.  Only the function's
	 * body should observe the change, and peer workers do not share in the
	 * execution of a function call started by this worker.
	 *
	 * Other changes might need to affect other workers, so forbid them.
	 */
	if (IsInParallelMode() && changeVal && action != GUC_ACTION_SAVE)
		ereport(elevel,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot set parameters during a parallel operation")));

	record = find_option(name, true, true, elevel);
	if (record == NULL)
		return 0;

	/*
	 * Check if the option can be set at this time. See guc.h for the precise
	 * rules.
	 */
	switch (record->context)
	{
		case PGC_INTERNAL:
			if (context != PGC_INTERNAL)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed",
								name)));
				return 0;
			}
			break;
		case PGC_POSTMASTER:
			if (context == PGC_SIGHUP)
			{
				/*
				 * We are re-reading a PGC_POSTMASTER variable from
				 * postgresql.conf.  We can't change the setting, so we should
				 * give a warning if the DBA tries to change it.  However,
				 * because of variant formats, canonicalization by check
				 * hooks, etc, we can't just compare the given string directly
				 * to what's stored.  Set a flag to check below after we have
				 * the final storable value.
				 */
				prohibitValueChange = true;
			}
			else if (context != PGC_POSTMASTER)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed without restarting the server",
								name)));
				return 0;
			}
			break;
		case PGC_SIGHUP:
			if (context != PGC_SIGHUP && context != PGC_POSTMASTER)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed now",
								name)));
				return 0;
			}

			/*
			 * Hmm, the idea of the SIGHUP context is "ought to be global, but
			 * can be changed after postmaster start". But there's nothing
			 * that prevents a crafty administrator from sending SIGHUP
			 * signals to individual backends only.
			 */
			break;
		case PGC_SU_BACKEND:
			/* Reject if we're connecting but user is not superuser */
			if (context == PGC_BACKEND)
			{
				ereport(elevel,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to set parameter \"%s\"",
								name)));
				return 0;
			}
			/* fall through to process the same as PGC_BACKEND */
			/* FALLTHROUGH */
		case PGC_BACKEND:
			if (context == PGC_SIGHUP)
			{
				/*
				 * If a PGC_BACKEND or PGC_SU_BACKEND parameter is changed in
				 * the config file, we want to accept the new value in the
				 * postmaster (whence it will propagate to
				 * subsequently-started backends), but ignore it in existing
				 * backends.  This is a tad klugy, but necessary because we
				 * don't re-read the config file during backend start.
				 *
				 * In EXEC_BACKEND builds, this works differently: we load all
				 * non-default settings from the CONFIG_EXEC_PARAMS file
				 * during backend start.  In that case we must accept
				 * PGC_SIGHUP settings, so as to have the same value as if
				 * we'd forked from the postmaster.  This can also happen when
				 * using RestoreGUCState() within a background worker that
				 * needs to have the same settings as the user backend that
				 * started it. is_reload will be true when either situation
				 * applies.
				 */
				if (IsUnderPostmaster && !is_reload)
					return -1;
			}
			else if (context != PGC_POSTMASTER &&
					 context != PGC_BACKEND &&
					 context != PGC_SU_BACKEND &&
					 source != PGC_S_CLIENT)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be set after connection start",
								name)));
				return 0;
			}
			break;
		case PGC_SUSET:
			if (context == PGC_USERSET || context == PGC_BACKEND)
			{
				ereport(elevel,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to set parameter \"%s\"",
								name)));
				return 0;
			}
			break;
		case PGC_USERSET:
			/* always okay */
			break;
	}

	/*
	 * Disallow changing GUC_NOT_WHILE_SEC_REST values if we are inside a
	 * security restriction context.  We can reject this regardless of the GUC
	 * context or source, mainly because sources that it might be reasonable
	 * to override for won't be seen while inside a function.
	 *
	 * Note: variables marked GUC_NOT_WHILE_SEC_REST should usually be marked
	 * GUC_NO_RESET_ALL as well, because ResetAllOptions() doesn't check this.
	 * An exception might be made if the reset value is assumed to be "safe".
	 *
	 * Note: this flag is currently used for "session_authorization" and
	 * "role".  We need to prohibit changing these inside a local userid
	 * context because when we exit it, GUC won't be notified, leaving things
	 * out of sync.  (This could be fixed by forcing a new GUC nesting level,
	 * but that would change behavior in possibly-undesirable ways.)  Also, we
	 * prohibit changing these in a security-restricted operation because
	 * otherwise RESET could be used to regain the session user's privileges.
	 */
	if (record->flags & GUC_NOT_WHILE_SEC_REST)
	{
		if (InLocalUserIdChange())
		{
			/*
			 * Phrasing of this error message is historical, but it's the most
			 * common case.
			 */
			ereport(elevel,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("cannot set parameter \"%s\" within security-definer function",
							name)));
			return 0;
		}
		if (InSecurityRestrictedOperation())
		{
			ereport(elevel,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("cannot set parameter \"%s\" within security-restricted operation",
							name)));
			return 0;
		}
	}

	/*
	 * Should we set reset/stacked values?	(If so, the behavior is not
	 * transactional.)	This is done either when we get a default value from
	 * the database's/user's/client's default settings or when we reset a
	 * value to its default.
	 */
	makeDefault = changeVal && (source <= PGC_S_OVERRIDE) &&
		((value != NULL) || source == PGC_S_DEFAULT);

	/*
	 * Ignore attempted set if overridden by previously processed setting.
	 * However, if changeVal is false then plow ahead anyway since we are
	 * trying to find out if the value is potentially good, not actually use
	 * it. Also keep going if makeDefault is true, since we may want to set
	 * the reset/stacked values even if we can't set the variable itself.
	 */
	if (record->source > source)
	{
		if (changeVal && !makeDefault)
		{
			elog(DEBUG3, "\"%s\": setting ignored because previous source is higher priority",
				 name);
			return -1;
		}
		changeVal = false;
	}

	/*
	 * Evaluate value and set variable.
	 */
	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;

#define newval (newval_union.boolval)

				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_bool_check_hook(conf, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
					context = conf->gen.reset_scontext;
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(&conf->gen, newextra))
						free(newextra);

					if (*conf->variable != newval)
					{
						record->status |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										name)));
						return 0;
					}
					record->status &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
					conf->gen.scontext = context;
				}
				if (makeDefault)
				{
					GucStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
						conf->gen.reset_scontext = context;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.boolval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;

#undef newval
			}

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) record;

#define newval (newval_union.intval)

				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_int_check_hook(conf, &newval, &newextra,
											 source, elevel))
						return 0;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
					context = conf->gen.reset_scontext;
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(&conf->gen, newextra))
						free(newextra);

					if (*conf->variable != newval)
					{
						record->status |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										name)));
						return 0;
					}
					record->status &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
					conf->gen.scontext = context;
				}
				if (makeDefault)
				{
					GucStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
						conf->gen.reset_scontext = context;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.intval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;

#undef newval
			}

		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;

#define newval (newval_union.realval)

				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_real_check_hook(conf, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
					context = conf->gen.reset_scontext;
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(&conf->gen, newextra))
						free(newextra);

					if (*conf->variable != newval)
					{
						record->status |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										name)));
						return 0;
					}
					record->status &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
					conf->gen.scontext = context;
				}
				if (makeDefault)
				{
					GucStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
						conf->gen.reset_scontext = context;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.realval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;

#undef newval
			}

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

#define newval (newval_union.stringval)

				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					/* non-NULL boot_val must always get strdup'd */
					if (conf->boot_val != NULL)
					{
						newval = guc_strdup(elevel, conf->boot_val);
						if (newval == NULL)
							return 0;
					}
					else
						newval = NULL;

					if (!call_string_check_hook(conf, &newval, &newextra,
												source, elevel))
					{
						free(newval);
						return 0;
					}
				}
				else
				{
					/*
					 * strdup not needed, since reset_val is already under
					 * guc.c's control
					 */
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
					context = conf->gen.reset_scontext;
				}

				if (prohibitValueChange)
				{
					bool		newval_different;

					/* newval shouldn't be NULL, so we're a bit sloppy here */
					newval_different = (*conf->variable == NULL ||
										newval == NULL ||
										strcmp(*conf->variable, newval) != 0);

					/* Release newval, unless it's reset_val */
					if (newval && !string_field_used(conf, newval))
						free(newval);
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(&conf->gen, newextra))
						free(newextra);

					if (newval_different)
					{
						record->status |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										name)));
						return 0;
					}
					record->status &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					set_string_field(conf, conf->variable, newval);
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
					conf->gen.scontext = context;
				}

				if (makeDefault)
				{
					GucStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						set_string_field(conf, &conf->reset_val, newval);
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
						conf->gen.reset_scontext = context;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							set_string_field(conf, &stack->prior.val.stringval,
											 newval);
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
						}
					}
				}

				/* Perhaps we didn't install newval anywhere */
				if (newval && !string_field_used(conf, newval))
					free(newval);
				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;

#undef newval
			}

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;

#define newval (newval_union.enumval)

				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_enum_check_hook(conf, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
					context = conf->gen.reset_scontext;
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(&conf->gen, newextra))
						free(newextra);

					if (*conf->variable != newval)
					{
						record->status |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										name)));
						return 0;
					}
					record->status &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
					conf->gen.scontext = context;
				}
				if (makeDefault)
				{
					GucStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
						conf->gen.reset_scontext = context;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.enumval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;

#undef newval
			}
	}

	if (changeVal && (record->flags & GUC_REPORT))
	{
		record->status |= GUC_NEEDS_REPORT;
		report_needed = true;
	}

	return changeVal ? 1 : -1;
}


/*
 * Set the fields for source file and line number the setting came from.
 */
static void
set_config_sourcefile(const char *name, char *sourcefile, int sourceline)
{
	struct config_generic *record;
	int			elevel;

	/*
	 * To avoid cluttering the log, only the postmaster bleats loudly about
	 * problems with the config file.
	 */
	elevel = IsUnderPostmaster ? DEBUG3 : LOG;

	record = find_option(name, true, false, elevel);
	/* should not happen */
	if (record == NULL)
		return;

	sourcefile = guc_strdup(elevel, sourcefile);
	if (record->sourcefile)
		free(record->sourcefile);
	record->sourcefile = sourcefile;
	record->sourceline = sourceline;
}

/*
 * Set a config option to the given value.
 *
 * See also set_config_option; this is just the wrapper to be called from
 * outside GUC.  (This function should be used when possible, because its API
 * is more stable than set_config_option's.)
 *
 * Note: there is no support here for setting source file/line, as it
 * is currently not needed.
 */
void
SetConfigOption(const char *name, const char *value,
				GucContext context, GucSource source)
{
	(void) set_config_option(name, value, context, source,
							 GUC_ACTION_SET, true, 0, false);
}



/*
 * Fetch the current value of the option `name', as a string.
 *
 * If the option doesn't exist, return NULL if missing_ok is true (NOTE that
 * this cannot be distinguished from a string variable with a NULL value!),
 * otherwise throw an ereport and don't return.
 *
 * If restrict_privileged is true, we also enforce that only superusers and
 * members of the pg_read_all_settings role can see GUC_SUPERUSER_ONLY
 * variables.  This should only be passed as true in user-driven calls.
 *
 * The string is *not* allocated for modification and is really only
 * valid until the next call to configuration related functions.
 */
const char *
GetConfigOption(const char *name, bool missing_ok, bool restrict_privileged)
{
	struct config_generic *record;
	static __thread char buffer[256];

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
		return NULL;
	if (restrict_privileged &&
		(record->flags & GUC_SUPERUSER_ONLY) &&
		!is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser or a member of pg_read_all_settings to examine \"%s\"",
						name)));

	switch (record->vartype)
	{
		case PGC_BOOL:
			return *((struct config_bool *) record)->variable ? "on" : "off";

		case PGC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 *((struct config_int *) record)->variable);
			return buffer;

		case PGC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 *((struct config_real *) record)->variable);
			return buffer;

		case PGC_STRING:
			return *((struct config_string *) record)->variable;

		case PGC_ENUM:
			return config_enum_lookup_by_value((struct config_enum *) record,
											   *((struct config_enum *) record)->variable);
	}
	return NULL;
}

/*
 * Get the RESET value associated with the given option.
 *
 * Note: this is not re-entrant, due to use of static result buffer;
 * not to mention that a string variable could have its reset_val changed.
 * Beware of assuming the result value is good for very long.
 */
const char *
GetConfigOptionResetString(const char *name)
{
	struct config_generic *record;
	static __thread char buffer[256];

	record = find_option(name, false, false, ERROR);
	Assert(record != NULL);
	if ((record->flags & GUC_SUPERUSER_ONLY) &&
		!is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser or a member of pg_read_all_settings to examine \"%s\"",
						name)));

	switch (record->vartype)
	{
		case PGC_BOOL:
			return ((struct config_bool *) record)->reset_val ? "on" : "off";

		case PGC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 ((struct config_int *) record)->reset_val);
			return buffer;

		case PGC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 ((struct config_real *) record)->reset_val);
			return buffer;

		case PGC_STRING:
			return ((struct config_string *) record)->reset_val;

		case PGC_ENUM:
			return config_enum_lookup_by_value((struct config_enum *) record,
											   ((struct config_enum *) record)->reset_val);
	}
	return NULL;
}

/*
 * Get the GUC flags associated with the given option.
 *
 * If the option doesn't exist, return 0 if missing_ok is true,
 * otherwise throw an ereport and don't return.
 */
int
GetConfigOptionFlags(const char *name, bool missing_ok)
{
	struct config_generic *record;

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
		return 0;
	return record->flags;
}


/*
 * flatten_set_variable_args
 *		Given a parsenode List as emitted by the grammar for SET,
 *		convert to the flat string representation used by GUC.
 *
 * We need to be told the name of the variable the args are for, because
 * the flattening rules vary (ugh).
 *
 * The result is NULL if args is NIL (i.e., SET ... TO DEFAULT), otherwise
 * a palloc'd string.
 */
static char *
flatten_set_variable_args(const char *name, List *args)
{
	struct config_generic *record;
	int			flags;
	StringInfoData buf;
	ListCell   *l;

	/* Fast path if just DEFAULT */
	if (args == NIL)
		return NULL;

	/*
	 * Get flags for the variable; if it's not known, use default flags.
	 * (Caller might throw error later, but not our business to do so here.)
	 */
	record = find_option(name, false, true, WARNING);
	if (record)
		flags = record->flags;
	else
		flags = 0;

	/* Complain if list input and non-list variable */
	if ((flags & GUC_LIST_INPUT) == 0 &&
		list_length(args) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SET %s takes only one argument", name)));

	initStringInfo(&buf);

	/*
	 * Each list member may be a plain A_Const node, or an A_Const within a
	 * TypeCast; the latter case is supported only for ConstInterval arguments
	 * (for SET TIME ZONE).
	 */
	foreach(l, args)
	{
		Node	   *arg = (Node *) lfirst(l);
		char	   *val;
		TypeName   *typeName = NULL;
		A_Const    *con;

		if (l != list_head(args))
			appendStringInfoString(&buf, ", ");

		if (IsA(arg, TypeCast))
		{
			TypeCast   *tc = (TypeCast *) arg;

			arg = tc->arg;
			typeName = tc->typeName;
		}

		if (!IsA(arg, A_Const))
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(arg));
		con = (A_Const *) arg;

		switch (nodeTag(&con->val))
		{
			case T_Integer:
				appendStringInfo(&buf, "%d", intVal(&con->val));
				break;
			case T_Float:
				/* represented as a string, so just copy it */
				appendStringInfoString(&buf, strVal(&con->val));
				break;
			case T_String:
				val = strVal(&con->val);
				if (typeName != NULL)
				{
					/*
					 * Must be a ConstInterval argument for TIME ZONE. Coerce
					 * to interval and back to normalize the value and account
					 * for any typmod.
					 */
					Oid			typoid;
					int32		typmod;
					Datum		interval;
					char	   *intervalout;

					typenameTypeIdAndMod(NULL, typeName, &typoid, &typmod);
					Assert(typoid == INTERVALOID);

					interval =
						DirectFunctionCall3(interval_in,
											CStringGetDatum(val),
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(typmod));

					intervalout =
						DatumGetCString(DirectFunctionCall1(interval_out,
															interval));
					appendStringInfo(&buf, "INTERVAL '%s'", intervalout);
				}
				else
				{
					/*
					 * Plain string literal or identifier.  For quote mode,
					 * quote it if it's not a vanilla identifier.
					 */
					if (flags & GUC_LIST_QUOTE)
						appendStringInfoString(&buf, quote_identifier(val));
					else
						appendStringInfoString(&buf, val);
				}
				break;
			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(&con->val));
				break;
		}
	}

	return buf.data;
}

/*
 * Write updated configuration parameter values into a temporary file.
 * This function traverses the list of parameters and quotes the string
 * values before writing them.
 */
static void
write_auto_conf_file(int fd, const char *filename, ConfigVariable *head)
{
	StringInfoData buf;
	ConfigVariable *item;

	initStringInfo(&buf);

	/* Emit file header containing warning comment */
	appendStringInfoString(&buf, "# Do not edit this file manually!\n");
	appendStringInfoString(&buf, "# It will be overwritten by the ALTER SYSTEM command.\n");

	errno = 0;
	if (write(fd, buf.data, buf.len) != buf.len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", filename)));
	}

	/* Emit each parameter, properly quoting the value */
	for (item = head; item != NULL; item = item->next)
	{
		char	   *escaped;

		resetStringInfo(&buf);

		appendStringInfoString(&buf, item->name);
		appendStringInfoString(&buf, " = '");

		escaped = escape_single_quotes_ascii(item->value);
		if (!escaped)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		appendStringInfoString(&buf, escaped);
		free(escaped);

		appendStringInfoString(&buf, "'\n");

		errno = 0;
		if (write(fd, buf.data, buf.len) != buf.len)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", filename)));
		}
	}

	/* fsync before considering the write to be successful */
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", filename)));

	pfree(buf.data);
}

/*
 * Update the given list of configuration parameters, adding, replacing
 * or deleting the entry for item "name" (delete if "value" == NULL).
 */
static void
replace_auto_config_value(ConfigVariable **head_p, ConfigVariable **tail_p,
						  const char *name, const char *value)
{
	ConfigVariable *item,
			   *next,
			   *prev = NULL;

	/*
	 * Remove any existing match(es) for "name".  Normally there'd be at most
	 * one, but if external tools have modified the config file, there could
	 * be more.
	 */
	for (item = *head_p; item != NULL; item = next)
	{
		next = item->next;
		if (guc_name_compare(item->name, name) == 0)
		{
			/* found a match, delete it */
			if (prev)
				prev->next = next;
			else
				*head_p = next;
			if (next == NULL)
				*tail_p = prev;

			pfree(item->name);
			pfree(item->value);
			pfree(item->filename);
			pfree(item);
		}
		else
			prev = item;
	}

	/* Done if we're trying to delete it */
	if (value == NULL)
		return;

	/* OK, append a new entry */
	item = palloc(sizeof *item);
	item->name = pstrdup(name);
	item->value = pstrdup(value);
	item->errmsg = NULL;
	item->filename = pstrdup("");	/* new item has no location */
	item->sourceline = 0;
	item->ignore = false;
	item->applied = false;
	item->next = NULL;

	if (*head_p == NULL)
		*head_p = item;
	else
		(*tail_p)->next = item;
	*tail_p = item;
}


/*
 * Execute ALTER SYSTEM statement.
 *
 * Read the old PG_AUTOCONF_FILENAME file, merge in the new variable value,
 * and write out an updated file.  If the command is ALTER SYSTEM RESET ALL,
 * we can skip reading the old file and just write an empty file.
 *
 * An LWLock is used to serialize updates of the configuration file.
 *
 * In case of an error, we leave the original automatic
 * configuration file (PG_AUTOCONF_FILENAME) intact.
 */
void
AlterSystemSetConfigFile(AlterSystemStmt *altersysstmt)
{
	char	   *name;
	char	   *value;
	bool		resetall = false;
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	volatile int Tmpfd;
	char		AutoConfFileName[MAXPGPATH];
	char		AutoConfTmpFileName[MAXPGPATH];

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to execute ALTER SYSTEM command")));

	/*
	 * Extract statement arguments
	 */
	name = altersysstmt->setstmt->name;

	switch (altersysstmt->setstmt->kind)
	{
		case VAR_SET_VALUE:
			value = ExtractSetVariableArgs(altersysstmt->setstmt);
			break;

		case VAR_SET_DEFAULT:
		case VAR_RESET:
			value = NULL;
			break;

		case VAR_RESET_ALL:
			value = NULL;
			resetall = true;
			break;

		default:
			elog(ERROR, "unrecognized alter system stmt type: %d",
				 altersysstmt->setstmt->kind);
			break;
	}

	/*
	 * Unless it's RESET_ALL, validate the target variable and value
	 */
	if (!resetall)
	{
		struct config_generic *record;

		record = find_option(name, false, false, ERROR);
		Assert(record != NULL);

		/*
		 * Don't allow parameters that can't be set in configuration files to
		 * be set in PG_AUTOCONF_FILENAME file.
		 */
		if ((record->context == PGC_INTERNAL) ||
			(record->flags & GUC_DISALLOW_IN_FILE) ||
			(record->flags & GUC_DISALLOW_IN_AUTO_FILE))
			ereport(ERROR,
					(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
					 errmsg("parameter \"%s\" cannot be changed",
							name)));

		/*
		 * If a value is specified, verify that it's sane.
		 */
		if (value)
		{
			union config_var_val newval;
			void	   *newextra = NULL;

			/* Check that it's acceptable for the indicated parameter */
			if (!parse_and_validate_value(record, name, value,
										  PGC_S_FILE, ERROR,
										  &newval, &newextra))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
								name, value)));

			if (record->vartype == PGC_STRING && newval.stringval != NULL)
				free(newval.stringval);
			if (newextra)
				free(newextra);

			/*
			 * We must also reject values containing newlines, because the
			 * grammar for config files doesn't support embedded newlines in
			 * string literals.
			 */
			if (strchr(value, '\n'))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("parameter value for ALTER SYSTEM must not contain a newline")));
		}
	}

	/*
	 * PG_AUTOCONF_FILENAME and its corresponding temporary file are always in
	 * the data directory, so we can reference them by simple relative paths.
	 */
	snprintf(AutoConfFileName, sizeof(AutoConfFileName), "%s",
			 PG_AUTOCONF_FILENAME);
	snprintf(AutoConfTmpFileName, sizeof(AutoConfTmpFileName), "%s.%s",
			 AutoConfFileName,
			 "tmp");

	/*
	 * Only one backend is allowed to operate on PG_AUTOCONF_FILENAME at a
	 * time.  Use AutoFileLock to ensure that.  We must hold the lock while
	 * reading the old file contents.
	 */
	LWLockAcquire(AutoFileLock, LW_EXCLUSIVE);

	/*
	 * If we're going to reset everything, then no need to open or parse the
	 * old file.  We'll just write out an empty list.
	 */
	if (!resetall)
	{
		struct stat st;

		if (stat(AutoConfFileName, &st) == 0)
		{
			/* open old file PG_AUTOCONF_FILENAME */
			FILE	   *infile;

			infile = AllocateFile(AutoConfFileName, "r");
			if (infile == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m",
								AutoConfFileName)));

			/* parse it */
			if (!ParseConfigFp(infile, AutoConfFileName, 0, LOG, &head, &tail))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("could not parse contents of file \"%s\"",
								AutoConfFileName)));

			FreeFile(infile);
		}

		/*
		 * Now, replace any existing entry with the new value, or add it if
		 * not present.
		 */
		replace_auto_config_value(&head, &tail, name, value);
	}

	/*
	 * To ensure crash safety, first write the new file data to a temp file,
	 * then atomically rename it into place.
	 *
	 * If there is a temp file left over due to a previous crash, it's okay to
	 * truncate and reuse it.
	 */
	Tmpfd = BasicOpenFile(AutoConfTmpFileName,
						  O_CREAT | O_RDWR | O_TRUNC);
	if (Tmpfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						AutoConfTmpFileName)));

	/*
	 * Use a TRY block to clean up the file if we fail.  Since we need a TRY
	 * block anyway, OK to use BasicOpenFile rather than OpenTransientFile.
	 */
	PG_TRY();
	{
		/* Write and sync the new contents to the temporary file */
		write_auto_conf_file(Tmpfd, AutoConfTmpFileName, head);

		/* Close before renaming; may be required on some platforms */
		close(Tmpfd);
		Tmpfd = -1;

		/*
		 * As the rename is atomic operation, if any problem occurs after this
		 * at worst it can lose the parameters set by last ALTER SYSTEM
		 * command.
		 */
		durable_rename(AutoConfTmpFileName, AutoConfFileName, ERROR);
	}
	PG_CATCH();
	{
		/* Close file first, else unlink might fail on some platforms */
		if (Tmpfd >= 0)
			close(Tmpfd);

		/* Unlink, but ignore any error */
		(void) unlink(AutoConfTmpFileName);

		PG_RE_THROW();
	}
	PG_END_TRY();

	FreeConfigVariables(head);

	LWLockRelease(AutoFileLock);
}

/*
 * SET command
 */
void
ExecSetVariableStmt(VariableSetStmt *stmt, bool isTopLevel)
{
	GucAction	action = stmt->is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET;

	/*
	 * Workers synchronize these parameters at the start of the parallel
	 * operation; then, we block SET during the operation.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot set parameters during a parallel operation")));

	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
		case VAR_SET_CURRENT:
			if (stmt->is_local)
				WarnNoTransactionBlock(isTopLevel, "SET LOCAL");
			(void) set_config_option(stmt->name,
									 ExtractSetVariableArgs(stmt),
									 (superuser() ? PGC_SUSET : PGC_USERSET),
									 PGC_S_SESSION,
									 action, true, 0, false);
			break;
		case VAR_SET_MULTI:

			/*
			 * Special-case SQL syntaxes.  The TRANSACTION and SESSION
			 * CHARACTERISTICS cases effectively set more than one variable
			 * per statement.  TRANSACTION SNAPSHOT only takes one argument,
			 * but we put it here anyway since it's a special case and not
			 * related to any GUC variable.
			 */
			if (strcmp(stmt->name, "TRANSACTION") == 0)
			{
				ListCell   *head;

				WarnNoTransactionBlock(isTopLevel, "SET TRANSACTION");

				foreach(head, stmt->args)
				{
					DefElem    *item = (DefElem *) lfirst(head);

					if (strcmp(item->defname, "transaction_isolation") == 0)
						SetPGVariable("transaction_isolation",
									  list_make1(item->arg), stmt->is_local);
					else if (strcmp(item->defname, "transaction_read_only") == 0)
						SetPGVariable("transaction_read_only",
									  list_make1(item->arg), stmt->is_local);
					else if (strcmp(item->defname, "transaction_deferrable") == 0)
						SetPGVariable("transaction_deferrable",
									  list_make1(item->arg), stmt->is_local);
					else
						elog(ERROR, "unexpected SET TRANSACTION element: %s",
							 item->defname);
				}
			}
			else if (strcmp(stmt->name, "SESSION CHARACTERISTICS") == 0)
			{
				ListCell   *head;

				foreach(head, stmt->args)
				{
					DefElem    *item = (DefElem *) lfirst(head);

					if (strcmp(item->defname, "transaction_isolation") == 0)
						SetPGVariable("default_transaction_isolation",
									  list_make1(item->arg), stmt->is_local);
					else if (strcmp(item->defname, "transaction_read_only") == 0)
						SetPGVariable("default_transaction_read_only",
									  list_make1(item->arg), stmt->is_local);
					else if (strcmp(item->defname, "transaction_deferrable") == 0)
						SetPGVariable("default_transaction_deferrable",
									  list_make1(item->arg), stmt->is_local);
					else
						elog(ERROR, "unexpected SET SESSION element: %s",
							 item->defname);
				}
			}
			else if (strcmp(stmt->name, "TRANSACTION SNAPSHOT") == 0)
			{
				A_Const    *con = linitial_node(A_Const, stmt->args);

				if (stmt->is_local)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("SET LOCAL TRANSACTION SNAPSHOT is not implemented")));

				WarnNoTransactionBlock(isTopLevel, "SET TRANSACTION");
				Assert(nodeTag(&con->val) == T_String);
				ImportSnapshot(strVal(&con->val));
			}
			else
				elog(ERROR, "unexpected SET MULTI element: %s",
					 stmt->name);
			break;
		case VAR_SET_DEFAULT:
			if (stmt->is_local)
				WarnNoTransactionBlock(isTopLevel, "SET LOCAL");
			/* fall through */
		case VAR_RESET:
			if (strcmp(stmt->name, "transaction_isolation") == 0)
				WarnNoTransactionBlock(isTopLevel, "RESET TRANSACTION");

			(void) set_config_option(stmt->name,
									 NULL,
									 (superuser() ? PGC_SUSET : PGC_USERSET),
									 PGC_S_SESSION,
									 action, true, 0, false);
			break;
		case VAR_RESET_ALL:
			ResetAllOptions();
			break;
	}
}

/*
 * Get the value to assign for a VariableSetStmt, or NULL if it's RESET.
 * The result is palloc'd.
 *
 * This is exported for use by actions such as ALTER ROLE SET.
 */
char *
ExtractSetVariableArgs(VariableSetStmt *stmt)
{
	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
			return flatten_set_variable_args(stmt->name, stmt->args);
		case VAR_SET_CURRENT:
			return GetConfigOptionByName(stmt->name, NULL, false);
		default:
			return NULL;
	}
}

/*
 * SetPGVariable - SET command exported as an easily-C-callable function.
 *
 * This provides access to SET TO value, as well as SET TO DEFAULT (expressed
 * by passing args == NIL), but not SET FROM CURRENT functionality.
 */
void
SetPGVariable(const char *name, List *args, bool is_local)
{
	char	   *argstring = flatten_set_variable_args(name, args);

	/* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
	(void) set_config_option(name,
							 argstring,
							 (superuser() ? PGC_SUSET : PGC_USERSET),
							 PGC_S_SESSION,
							 is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET,
							 true, 0, false);
}

/*
 * SET command wrapped as a SQL callable function.
 */
Datum
set_config_by_name(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *value;
	char	   *new_value;
	bool		is_local;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("SET requires parameter name")));

	/* Get the GUC variable name */
	name = TextDatumGetCString(PG_GETARG_DATUM(0));

	/* Get the desired value or set to NULL for a reset request */
	if (PG_ARGISNULL(1))
		value = NULL;
	else
		value = TextDatumGetCString(PG_GETARG_DATUM(1));

	/*
	 * Get the desired state of is_local. Default to false if provided value
	 * is NULL
	 */
	if (PG_ARGISNULL(2))
		is_local = false;
	else
		is_local = PG_GETARG_BOOL(2);

	/* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
	(void) set_config_option(name,
							 value,
							 (superuser() ? PGC_SUSET : PGC_USERSET),
							 PGC_S_SESSION,
							 is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET,
							 true, 0, false);

	/* get the new current value */
	new_value = GetConfigOptionByName(name, NULL, false);

	/* Convert return string to text */
	PG_RETURN_TEXT_P(cstring_to_text(new_value));
}


/*
 * Common code for DefineCustomXXXVariable subroutines: allocate the
 * new variable's config struct and fill in generic fields.
 */
static struct config_generic *
init_custom_variable(const char *name,
					 const char *short_desc,
					 const char *long_desc,
					 GucContext context,
					 int flags,
					 enum config_type type,
					 size_t sz)
{
	struct config_generic *gen;

	/*
	 * Only allow custom PGC_POSTMASTER variables to be created during shared
	 * library preload; any later than that, we can't ensure that the value
	 * doesn't change after startup.  This is a fatal elog if it happens; just
	 * erroring out isn't safe because we don't know what the calling loadable
	 * module might already have hooked into.
	 */
	if (context == PGC_POSTMASTER &&
		!process_shared_preload_libraries_in_progress)
		elog(FATAL, "cannot create PGC_POSTMASTER variables after startup");

	/*
	 * We can't support custom GUC_LIST_QUOTE variables, because the wrong
	 * things would happen if such a variable were set or pg_dump'd when the
	 * defining extension isn't loaded.  Again, treat this as fatal because
	 * the loadable module may be partly initialized already.
	 */
	if (flags & GUC_LIST_QUOTE)
		elog(FATAL, "extensions cannot define GUC_LIST_QUOTE variables");

	/*
	 * Before pljava commit 398f3b876ed402bdaec8bc804f29e2be95c75139
	 * (2015-12-15), two of that module's PGC_USERSET variables facilitated
	 * trivial escalation to superuser privileges.  Restrict the variables to
	 * protect sites that have yet to upgrade pljava.
	 */
	if (context == PGC_USERSET &&
		(strcmp(name, "pljava.classpath") == 0 ||
		 strcmp(name, "pljava.vmoptions") == 0))
		context = PGC_SUSET;

	gen = (struct config_generic *) guc_malloc(ERROR, sz);
	memset(gen, 0, sz);

	gen->name = guc_strdup(ERROR, name);
	gen->context = context;
	gen->group = CUSTOM_OPTIONS;
	gen->short_desc = short_desc;
	gen->long_desc = long_desc;
	gen->flags = flags;
	gen->vartype = type;

	return gen;
}

/*
 * Common code for DefineCustomXXXVariable subroutines: insert the new
 * variable into the GUC variable array, replacing any placeholder.
 */
static void
define_custom_variable(struct config_generic *variable)
{
	const char *name = variable->name;
	const char **nameAddr = &name;
	struct config_string *pHolder;
	struct config_generic **res;

	/*
	 * See if there's a placeholder by the same name.
	 */
	res = (struct config_generic **) bsearch((void *) &nameAddr,
											 (void *) guc_variables,
											 num_guc_variables,
											 sizeof(struct config_generic *),
											 guc_var_compare);
	if (res == NULL)
	{
		/*
		 * No placeholder to replace, so we can just add it ... but first,
		 * make sure it's initialized to its default value.
		 */
		InitializeOneGUCOption(variable);
		add_guc_variable(variable, ERROR);
		return;
	}

	/*
	 * This better be a placeholder
	 */
	if (((*res)->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("attempt to redefine parameter \"%s\"", name)));

	Assert((*res)->vartype == PGC_STRING);
	pHolder = (struct config_string *) (*res);

	/*
	 * First, set the variable to its default value.  We must do this even
	 * though we intend to immediately apply a new value, since it's possible
	 * that the new value is invalid.
	 */
	InitializeOneGUCOption(variable);

	/*
	 * Replace the placeholder. We aren't changing the name, so no re-sorting
	 * is necessary
	 */
	*res = variable;

	/*
	 * Assign the string value(s) stored in the placeholder to the real
	 * variable.  Essentially, we need to duplicate all the active and stacked
	 * values, but with appropriate validation and datatype adjustment.
	 *
	 * If an assignment fails, we report a WARNING and keep going.  We don't
	 * want to throw ERROR for bad values, because it'd bollix the add-on
	 * module that's presumably halfway through getting loaded.  In such cases
	 * the default or previous state will become active instead.
	 */

	/* First, apply the reset value if any */
	if (pHolder->reset_val)
		(void) set_config_option(name, pHolder->reset_val,
								 pHolder->gen.reset_scontext,
								 pHolder->gen.reset_source,
								 GUC_ACTION_SET, true, WARNING, false);
	/* That should not have resulted in stacking anything */
	Assert(variable->stack == NULL);

	/* Now, apply current and stacked values, in the order they were stacked */
	reapply_stacked_values(variable, pHolder, pHolder->gen.stack,
						   *(pHolder->variable),
						   pHolder->gen.scontext, pHolder->gen.source);

	/* Also copy over any saved source-location information */
	if (pHolder->gen.sourcefile)
		set_config_sourcefile(name, pHolder->gen.sourcefile,
							  pHolder->gen.sourceline);

	/*
	 * Free up as much as we conveniently can of the placeholder structure.
	 * (This neglects any stack items, so it's possible for some memory to be
	 * leaked.  Since this can only happen once per session per variable, it
	 * doesn't seem worth spending much code on.)
	 */
	set_string_field(pHolder, pHolder->variable, NULL);
	set_string_field(pHolder, &pHolder->reset_val, NULL);

	free(pHolder);
}

/*
 * Recursive subroutine for define_custom_variable: reapply non-reset values
 *
 * We recurse so that the values are applied in the same order as originally.
 * At each recursion level, apply the upper-level value (passed in) in the
 * fashion implied by the stack entry.
 */
static void
reapply_stacked_values(struct config_generic *variable,
					   struct config_string *pHolder,
					   GucStack *stack,
					   const char *curvalue,
					   GucContext curscontext, GucSource cursource)
{
	const char *name = variable->name;
	GucStack   *oldvarstack = variable->stack;

	if (stack != NULL)
	{
		/* First, recurse, so that stack items are processed bottom to top */
		reapply_stacked_values(variable, pHolder, stack->prev,
							   stack->prior.val.stringval,
							   stack->scontext, stack->source);

		/* See how to apply the passed-in value */
		switch (stack->state)
		{
			case GUC_SAVE:
				(void) set_config_option(name, curvalue,
										 curscontext, cursource,
										 GUC_ACTION_SAVE, true,
										 WARNING, false);
				break;

			case GUC_SET:
				(void) set_config_option(name, curvalue,
										 curscontext, cursource,
										 GUC_ACTION_SET, true,
										 WARNING, false);
				break;

			case GUC_LOCAL:
				(void) set_config_option(name, curvalue,
										 curscontext, cursource,
										 GUC_ACTION_LOCAL, true,
										 WARNING, false);
				break;

			case GUC_SET_LOCAL:
				/* first, apply the masked value as SET */
				(void) set_config_option(name, stack->masked.val.stringval,
										 stack->masked_scontext, PGC_S_SESSION,
										 GUC_ACTION_SET, true,
										 WARNING, false);
				/* then apply the current value as LOCAL */
				(void) set_config_option(name, curvalue,
										 curscontext, cursource,
										 GUC_ACTION_LOCAL, true,
										 WARNING, false);
				break;
		}

		/* If we successfully made a stack entry, adjust its nest level */
		if (variable->stack != oldvarstack)
			variable->stack->nest_level = stack->nest_level;
	}
	else
	{
		/*
		 * We are at the end of the stack.  If the active/previous value is
		 * different from the reset value, it must represent a previously
		 * committed session value.  Apply it, and then drop the stack entry
		 * that set_config_option will have created under the impression that
		 * this is to be just a transactional assignment.  (We leak the stack
		 * entry.)
		 */
		if (curvalue != pHolder->reset_val ||
			curscontext != pHolder->gen.reset_scontext ||
			cursource != pHolder->gen.reset_source)
		{
			(void) set_config_option(name, curvalue,
									 curscontext, cursource,
									 GUC_ACTION_SET, true, WARNING, false);
			variable->stack = NULL;
		}
	}
}

void
DefineCustomBoolVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 bool *valueAddr,
						 bool bootValue,
						 GucContext context,
						 int flags,
						 GucBoolCheckHook check_hook,
						 GucBoolAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_bool *var;

	var = (struct config_bool *)
		init_custom_variable(name, short_desc, long_desc, context, flags,
							 PGC_BOOL, sizeof(struct config_bool));
	var->variable = valueAddr;
	var->boot_val = bootValue;
	var->reset_val = bootValue;
	var->check_hook = check_hook;
	var->assign_hook = assign_hook;
	var->show_hook = show_hook;
	define_custom_variable(&var->gen);
}

void
DefineCustomIntVariable(const char *name,
						const char *short_desc,
						const char *long_desc,
						int *valueAddr,
						int bootValue,
						int minValue,
						int maxValue,
						GucContext context,
						int flags,
						GucIntCheckHook check_hook,
						GucIntAssignHook assign_hook,
						GucShowHook show_hook)
{
	struct config_int *var;

	var = (struct config_int *)
		init_custom_variable(name, short_desc, long_desc, context, flags,
							 PGC_INT, sizeof(struct config_int));
	var->variable = valueAddr;
	var->boot_val = bootValue;
	var->reset_val = bootValue;
	var->min = minValue;
	var->max = maxValue;
	var->check_hook = check_hook;
	var->assign_hook = assign_hook;
	var->show_hook = show_hook;
	define_custom_variable(&var->gen);
}

void
DefineCustomRealVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 double *valueAddr,
						 double bootValue,
						 double minValue,
						 double maxValue,
						 GucContext context,
						 int flags,
						 GucRealCheckHook check_hook,
						 GucRealAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_real *var;

	var = (struct config_real *)
		init_custom_variable(name, short_desc, long_desc, context, flags,
							 PGC_REAL, sizeof(struct config_real));
	var->variable = valueAddr;
	var->boot_val = bootValue;
	var->reset_val = bootValue;
	var->min = minValue;
	var->max = maxValue;
	var->check_hook = check_hook;
	var->assign_hook = assign_hook;
	var->show_hook = show_hook;
	define_custom_variable(&var->gen);
}

void
DefineCustomStringVariable(const char *name,
						   const char *short_desc,
						   const char *long_desc,
						   char **valueAddr,
						   const char *bootValue,
						   GucContext context,
						   int flags,
						   GucStringCheckHook check_hook,
						   GucStringAssignHook assign_hook,
						   GucShowHook show_hook)
{
	struct config_string *var;

	var = (struct config_string *)
		init_custom_variable(name, short_desc, long_desc, context, flags,
							 PGC_STRING, sizeof(struct config_string));
	var->variable = valueAddr;
	var->boot_val = bootValue;
	var->check_hook = check_hook;
	var->assign_hook = assign_hook;
	var->show_hook = show_hook;
	define_custom_variable(&var->gen);
}

void
DefineCustomEnumVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 int *valueAddr,
						 int bootValue,
						 const struct config_enum_entry *options,
						 GucContext context,
						 int flags,
						 GucEnumCheckHook check_hook,
						 GucEnumAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_enum *var;

	var = (struct config_enum *)
		init_custom_variable(name, short_desc, long_desc, context, flags,
							 PGC_ENUM, sizeof(struct config_enum));
	var->variable = valueAddr;
	var->boot_val = bootValue;
	var->reset_val = bootValue;
	var->options = options;
	var->check_hook = check_hook;
	var->assign_hook = assign_hook;
	var->show_hook = show_hook;
	define_custom_variable(&var->gen);
}

void
EmitWarningsOnPlaceholders(const char *className)
{
	int			classLen = strlen(className);
	int			i;

	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *var = guc_variables[i];

		if ((var->flags & GUC_CUSTOM_PLACEHOLDER) != 0 &&
			strncmp(className, var->name, classLen) == 0 &&
			var->name[classLen] == GUC_QUALIFIER_SEPARATOR)
		{
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("unrecognized configuration parameter \"%s\"",
							var->name)));
		}
	}
}


/*
 * SHOW command
 */
void
GetPGVariable(const char *name, DestReceiver *dest)
{
	if (guc_name_compare(name, "all") == 0)
		ShowAllGUCConfig(dest);
	else
		ShowGUCConfigOption(name, dest);
}

TupleDesc
GetPGVariableResultDesc(const char *name)
{
	TupleDesc	tupdesc;

	if (guc_name_compare(name, "all") == 0)
	{
		/* need a tuple descriptor representing three TEXT columns */
		tupdesc = CreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "setting",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "description",
						   TEXTOID, -1, 0);
	}
	else
	{
		const char *varname;

		/* Get the canonical spelling of name */
		(void) GetConfigOptionByName(name, &varname, false);

		/* need a tuple descriptor representing a single TEXT column */
		tupdesc = CreateTemplateTupleDesc(1);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, varname,
						   TEXTOID, -1, 0);
	}
	return tupdesc;
}


/*
 * SHOW command
 */
static void
ShowGUCConfigOption(const char *name, DestReceiver *dest)
{
	TupOutputState *tstate;
	TupleDesc	tupdesc;
	const char *varname;
	char	   *value;

	/* Get the value and canonical spelling of name */
	value = GetConfigOptionByName(name, &varname, false);

	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, varname,
							  TEXTOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

	/* Send it */
	do_text_output_oneline(tstate, value);

	end_tup_output(tstate);
}

/*
 * SHOW ALL command
 */
static void
ShowAllGUCConfig(DestReceiver *dest)
{
	int			i;
	TupOutputState *tstate;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		isnull[3] = {false, false, false};

	/* need a tuple descriptor representing three TEXT columns */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "name",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 2, "setting",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 3, "description",
							  TEXTOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *conf = guc_variables[i];
		char	   *setting;

		if ((conf->flags & GUC_NO_SHOW_ALL) ||
			((conf->flags & GUC_SUPERUSER_ONLY) &&
			 !is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)))
			continue;

		/* assign to the values array */
		values[0] = PointerGetDatum(cstring_to_text(conf->name));

		setting = _ShowOption(conf, true);
		if (setting)
		{
			values[1] = PointerGetDatum(cstring_to_text(setting));
			isnull[1] = false;
		}
		else
		{
			values[1] = PointerGetDatum(NULL);
			isnull[1] = true;
		}

		values[2] = PointerGetDatum(cstring_to_text(conf->short_desc));

		/* send it to dest */
		do_tup_output(tstate, values, isnull);

		/* clean up */
		pfree(DatumGetPointer(values[0]));
		if (setting)
		{
			pfree(setting);
			pfree(DatumGetPointer(values[1]));
		}
		pfree(DatumGetPointer(values[2]));
	}

	end_tup_output(tstate);
}

/*
 * Return an array of modified GUC options to show in EXPLAIN.
 *
 * We only report options related to query planning (marked with GUC_EXPLAIN),
 * with values different from their built-in defaults.
 */
struct config_generic **
get_explain_guc_options(int *num)
{
	struct config_generic **result;

	*num = 0;

	/*
	 * While only a fraction of all the GUC variables are marked GUC_EXPLAIN,
	 * it doesn't seem worth dynamically resizing this array.
	 */
	result = palloc(sizeof(struct config_generic *) * num_guc_variables);

	for (int i = 0; i < num_guc_variables; i++)
	{
		bool		modified;
		struct config_generic *conf = guc_variables[i];

		/* return only parameters marked for inclusion in explain */
		if (!(conf->flags & GUC_EXPLAIN))
			continue;

		/* return only options visible to the current user */
		if ((conf->flags & GUC_NO_SHOW_ALL) ||
			((conf->flags & GUC_SUPERUSER_ONLY) &&
			 !is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)))
			continue;

		/* return only options that are different from their boot values */
		modified = false;

		switch (conf->vartype)
		{
			case PGC_BOOL:
				{
					struct config_bool *lconf = (struct config_bool *) conf;

					modified = (lconf->boot_val != *(lconf->variable));
				}
				break;

			case PGC_INT:
				{
					struct config_int *lconf = (struct config_int *) conf;

					modified = (lconf->boot_val != *(lconf->variable));
				}
				break;

			case PGC_REAL:
				{
					struct config_real *lconf = (struct config_real *) conf;

					modified = (lconf->boot_val != *(lconf->variable));
				}
				break;

			case PGC_STRING:
				{
					struct config_string *lconf = (struct config_string *) conf;

					modified = (strcmp(lconf->boot_val, *(lconf->variable)) != 0);
				}
				break;

			case PGC_ENUM:
				{
					struct config_enum *lconf = (struct config_enum *) conf;

					modified = (lconf->boot_val != *(lconf->variable));
				}
				break;

			default:
				elog(ERROR, "unexpected GUC type: %d", conf->vartype);
		}

		if (!modified)
			continue;

		/* OK, report it */
		result[*num] = conf;
		*num = *num + 1;
	}

	return result;
}

/*
 * Return GUC variable value by name; optionally return canonical form of
 * name.  If the GUC is unset, then throw an error unless missing_ok is true,
 * in which case return NULL.  Return value is palloc'd (but *varname isn't).
 */
char *
GetConfigOptionByName(const char *name, const char **varname, bool missing_ok)
{
	struct config_generic *record;

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
	{
		if (varname)
			*varname = NULL;
		return NULL;
	}

	if ((record->flags & GUC_SUPERUSER_ONLY) &&
		!is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser or a member of pg_read_all_settings to examine \"%s\"",
						name)));

	if (varname)
		*varname = record->name;

	return _ShowOption(record, true);
}

/*
 * Return GUC variable value by variable number; optionally return canonical
 * form of name.  Return value is palloc'd.
 */
void
GetConfigOptionByNum(int varnum, const char **values, bool *noshow)
{
	char		buffer[256];
	struct config_generic *conf;

	/* check requested variable number valid */
	Assert((varnum >= 0) && (varnum < num_guc_variables));

	conf = guc_variables[varnum];

	if (noshow)
	{
		if ((conf->flags & GUC_NO_SHOW_ALL) ||
			((conf->flags & GUC_SUPERUSER_ONLY) &&
			 !is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)))
			*noshow = true;
		else
			*noshow = false;
	}

	/* first get the generic attributes */

	/* name */
	values[0] = conf->name;

	/* setting: use _ShowOption in order to avoid duplicating the logic */
	values[1] = _ShowOption(conf, false);

	/* unit, if any (NULL is fine) */
	values[2] = get_config_unit_name(conf->flags);

	/* group */
	values[3] = _(config_group_names[conf->group]);

	/* short_desc */
	values[4] = _(conf->short_desc);

	/* extra_desc */
	values[5] = _(conf->long_desc);

	/* context */
	values[6] = GucContext_Names[conf->context];

	/* vartype */
	values[7] = config_type_names[conf->vartype];

	/* source */
	values[8] = GucSource_Names[conf->source];

	/* now get the type specific attributes */
	switch (conf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *lconf = (struct config_bool *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				values[12] = pstrdup(lconf->boot_val ? "on" : "off");

				/* reset_val */
				values[13] = pstrdup(lconf->reset_val ? "on" : "off");
			}
			break;

		case PGC_INT:
			{
				struct config_int *lconf = (struct config_int *) conf;

				/* min_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->min);
				values[9] = pstrdup(buffer);

				/* max_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->max);
				values[10] = pstrdup(buffer);

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->boot_val);
				values[12] = pstrdup(buffer);

				/* reset_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->reset_val);
				values[13] = pstrdup(buffer);
			}
			break;

		case PGC_REAL:
			{
				struct config_real *lconf = (struct config_real *) conf;

				/* min_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->min);
				values[9] = pstrdup(buffer);

				/* max_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->max);
				values[10] = pstrdup(buffer);

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->boot_val);
				values[12] = pstrdup(buffer);

				/* reset_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->reset_val);
				values[13] = pstrdup(buffer);
			}
			break;

		case PGC_STRING:
			{
				struct config_string *lconf = (struct config_string *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				if (lconf->boot_val == NULL)
					values[12] = NULL;
				else
					values[12] = pstrdup(lconf->boot_val);

				/* reset_val */
				if (lconf->reset_val == NULL)
					values[13] = NULL;
				else
					values[13] = pstrdup(lconf->reset_val);
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *lconf = (struct config_enum *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */

				/*
				 * NOTE! enumvals with double quotes in them are not
				 * supported!
				 */
				values[11] = config_enum_get_options((struct config_enum *) conf,
													 "{\"", "\"}", "\",\"");

				/* boot_val */
				values[12] = pstrdup(config_enum_lookup_by_value(lconf,
																 lconf->boot_val));

				/* reset_val */
				values[13] = pstrdup(config_enum_lookup_by_value(lconf,
																 lconf->reset_val));
			}
			break;

		default:
			{
				/*
				 * should never get here, but in case we do, set 'em to NULL
				 */

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				values[12] = NULL;

				/* reset_val */
				values[13] = NULL;
			}
			break;
	}

	/*
	 * If the setting came from a config file, set the source location. For
	 * security reasons, we don't show source file/line number for
	 * insufficiently-privileged users.
	 */
	if (conf->source == PGC_S_FILE &&
		is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS))
	{
		values[14] = conf->sourcefile;
		snprintf(buffer, sizeof(buffer), "%d", conf->sourceline);
		values[15] = pstrdup(buffer);
	}
	else
	{
		values[14] = NULL;
		values[15] = NULL;
	}

	values[16] = (conf->status & GUC_PENDING_RESTART) ? "t" : "f";
}

/*
 * Return the total number of GUC variables
 */
int
GetNumConfigOptions(void)
{
	return num_guc_variables;
}

/*
 * show_config_by_name - equiv to SHOW X command but implemented as
 * a function.
 */
Datum
show_config_by_name(PG_FUNCTION_ARGS)
{
	char	   *varname = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *varval;

	/* Get the value */
	varval = GetConfigOptionByName(varname, NULL, false);

	/* Convert to text */
	PG_RETURN_TEXT_P(cstring_to_text(varval));
}

/*
 * show_config_by_name_missing_ok - equiv to SHOW X command but implemented as
 * a function.  If X does not exist, suppress the error and just return NULL
 * if missing_ok is true.
 */
Datum
show_config_by_name_missing_ok(PG_FUNCTION_ARGS)
{
	char	   *varname = TextDatumGetCString(PG_GETARG_DATUM(0));
	bool		missing_ok = PG_GETARG_BOOL(1);
	char	   *varval;

	/* Get the value */
	varval = GetConfigOptionByName(varname, NULL, missing_ok);

	/* return NULL if no such variable */
	if (varval == NULL)
		PG_RETURN_NULL();

	/* Convert to text */
	PG_RETURN_TEXT_P(cstring_to_text(varval));
}

/*
 * show_all_settings - equiv to SHOW ALL command but implemented as
 * a Table Function.
 */
#define NUM_PG_SETTINGS_ATTS	17

Datum
show_all_settings(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc	tupdesc;
	int			call_cntr;
	int			max_calls;
	AttInMetadata *attinmeta;
	MemoryContext oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * need a tuple descriptor representing NUM_PG_SETTINGS_ATTS columns
		 * of the appropriate types
		 */
		tupdesc = CreateTemplateTupleDesc(NUM_PG_SETTINGS_ATTS);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "setting",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "unit",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "category",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "short_desc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "extra_desc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "context",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "vartype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "source",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "min_val",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "max_val",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "enumvals",
						   TEXTARRAYOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "boot_val",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "reset_val",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "sourcefile",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "sourceline",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "pending_restart",
						   BOOLOID, -1, 0);

		/*
		 * Generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* total number of tuples to be returned */
		funcctx->max_calls = GetNumConfigOptions();

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		char	   *values[NUM_PG_SETTINGS_ATTS];
		bool		noshow;
		HeapTuple	tuple;
		Datum		result;

		/*
		 * Get the next visible GUC variable name and value
		 */
		do
		{
			GetConfigOptionByNum(call_cntr, (const char **) values, &noshow);
			if (noshow)
			{
				/* bump the counter and get the next config setting */
				call_cntr = ++funcctx->call_cntr;

				/* make sure we haven't gone too far now */
				if (call_cntr >= max_calls)
					SRF_RETURN_DONE(funcctx);
			}
		} while (noshow);

		/* build a tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * show_all_file_settings
 *
 * Returns a table of all parameter settings in all configuration files
 * which includes the config file pathname, the line number, a sequence number
 * indicating the order in which the settings were encountered, the parameter
 * name and value, a bool showing if the value could be applied, and possibly
 * an associated error message.  (For problems such as syntax errors, the
 * parameter name/value might be NULL.)
 *
 * Note: no filtering is done here, instead we depend on the GRANT system
 * to prevent unprivileged users from accessing this function or the view
 * built on top of it.
 */
Datum
show_all_file_settings(PG_FUNCTION_ARGS)
{
#define NUM_PG_FILE_SETTINGS_ATTS 7
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	ConfigVariable *conf;
	int			seqno;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* Check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Scan the config files using current context as workspace */
	conf = ProcessConfigFileInternal(PGC_SIGHUP, false, DEBUG3);

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(NUM_PG_FILE_SETTINGS_ATTS);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "sourcefile",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "sourceline",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "seqno",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "name",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "setting",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "applied",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "error",
					   TEXTOID, -1, 0);

	/* Build a tuplestore to return our results in */
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	/* The rest can be done in short-lived context */
	MemoryContextSwitchTo(oldcontext);

	/* Process the results and create a tuplestore */
	for (seqno = 1; conf != NULL; conf = conf->next, seqno++)
	{
		Datum		values[NUM_PG_FILE_SETTINGS_ATTS];
		bool		nulls[NUM_PG_FILE_SETTINGS_ATTS];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* sourcefile */
		if (conf->filename)
			values[0] = PointerGetDatum(cstring_to_text(conf->filename));
		else
			nulls[0] = true;

		/* sourceline (not meaningful if no sourcefile) */
		if (conf->filename)
			values[1] = Int32GetDatum(conf->sourceline);
		else
			nulls[1] = true;

		/* seqno */
		values[2] = Int32GetDatum(seqno);

		/* name */
		if (conf->name)
			values[3] = PointerGetDatum(cstring_to_text(conf->name));
		else
			nulls[3] = true;

		/* setting */
		if (conf->value)
			values[4] = PointerGetDatum(cstring_to_text(conf->value));
		else
			nulls[4] = true;

		/* applied */
		values[5] = BoolGetDatum(conf->applied);

		/* error */
		if (conf->errmsg)
			values[6] = PointerGetDatum(cstring_to_text(conf->errmsg));
		else
			nulls[6] = true;

		/* shove row into tuplestore */
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

static char *
_ShowOption(struct config_generic *record, bool use_units)
{
	char		buffer[256];
	const char *val;

	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;

				if (conf->show_hook)
					val = conf->show_hook();
				else
					val = *conf->variable ? "on" : "off";
			}
			break;

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) record;

				if (conf->show_hook)
					val = conf->show_hook();
				else
				{
					/*
					 * Use int64 arithmetic to avoid overflows in units
					 * conversion.
					 */
					int64		result = *conf->variable;
					const char *unit;

					if (use_units && result > 0 && (record->flags & GUC_UNIT))
						convert_int_from_base_unit(result,
												   record->flags & GUC_UNIT,
												   &result, &unit);
					else
						unit = "";

					snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s",
							 result, unit);
					val = buffer;
				}
			}
			break;

		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;

				if (conf->show_hook)
					val = conf->show_hook();
				else
				{
					double		result = *conf->variable;
					const char *unit;

					if (use_units && result > 0 && (record->flags & GUC_UNIT))
						convert_real_from_base_unit(result,
													record->flags & GUC_UNIT,
													&result, &unit);
					else
						unit = "";

					snprintf(buffer, sizeof(buffer), "%g%s",
							 result, unit);
					val = buffer;
				}
			}
			break;

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

				if (conf->show_hook)
					val = conf->show_hook();
				else if (*conf->variable && **conf->variable)
					val = *conf->variable;
				else
					val = "";
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;

				if (conf->show_hook)
					val = conf->show_hook();
				else
					val = config_enum_lookup_by_value(conf, *conf->variable);
			}
			break;

		default:
			/* just to keep compiler quiet */
			val = "???";
			break;
	}

	return pstrdup(val);
}


#ifdef EXEC_BACKEND

/*
 *	These routines dump out all non-default GUC options into a binary
 *	file that is read by all exec'ed backends.  The format is:
 *
 *		variable name, string, null terminated
 *		variable value, string, null terminated
 *		variable sourcefile, string, null terminated (empty if none)
 *		variable sourceline, integer
 *		variable source, integer
 *		variable scontext, integer
 */
static void
write_one_nondefault_variable(FILE *fp, struct config_generic *gconf)
{
	if (gconf->source == PGC_S_DEFAULT)
		return;

	fprintf(fp, "%s", gconf->name);
	fputc(0, fp);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) gconf;

				if (*conf->variable)
					fprintf(fp, "true");
				else
					fprintf(fp, "false");
			}
			break;

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;

				fprintf(fp, "%d", *conf->variable);
			}
			break;

		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) gconf;

				fprintf(fp, "%.17g", *conf->variable);
			}
			break;

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;

				fprintf(fp, "%s", *conf->variable);
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;

				fprintf(fp, "%s",
						config_enum_lookup_by_value(conf, *conf->variable));
			}
			break;
	}

	fputc(0, fp);

	if (gconf->sourcefile)
		fprintf(fp, "%s", gconf->sourcefile);
	fputc(0, fp);

	fwrite(&gconf->sourceline, 1, sizeof(gconf->sourceline), fp);
	fwrite(&gconf->source, 1, sizeof(gconf->source), fp);
	fwrite(&gconf->scontext, 1, sizeof(gconf->scontext), fp);
}

void
write_nondefault_variables(GucContext context)
{
	int			elevel;
	FILE	   *fp;
	int			i;

	Assert(context == PGC_POSTMASTER || context == PGC_SIGHUP);

	elevel = (context == PGC_SIGHUP) ? LOG : ERROR;

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS_NEW, "w");
	if (!fp)
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	for (i = 0; i < num_guc_variables; i++)
	{
		write_one_nondefault_variable(fp, guc_variables[i]);
	}

	if (FreeFile(fp))
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	/*
	 * Put new file in place.  This could delay on Win32, but we don't hold
	 * any exclusive locks.
	 */
	rename(CONFIG_EXEC_PARAMS_NEW, CONFIG_EXEC_PARAMS);
}


/*
 *	Read string, including null byte from file
 *
 *	Return NULL on EOF and nothing read
 */
static char *
read_string_with_null(FILE *fp)
{
	int			i = 0,
				ch,
				maxlen = 256;
	char	   *str = NULL;

	do
	{
		if ((ch = fgetc(fp)) == EOF)
		{
			if (i == 0)
				return NULL;
			else
				elog(FATAL, "invalid format of exec config params file");
		}
		if (i == 0)
			str = guc_malloc(FATAL, maxlen);
		else if (i == maxlen)
			str = guc_realloc(FATAL, str, maxlen *= 2);
		str[i++] = ch;
	} while (ch != 0);

	return str;
}


/*
 *	This routine loads a previous postmaster dump of its non-default
 *	settings.
 */
void
read_nondefault_variables(void)
{
	FILE	   *fp;
	char	   *varname,
			   *varvalue,
			   *varsourcefile;
	int			varsourceline;
	GucSource	varsource;
	GucContext	varscontext;

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS, "r");
	if (!fp)
	{
		/* File not found is fine */
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": %m",
							CONFIG_EXEC_PARAMS)));
		return;
	}

	for (;;)
	{
		struct config_generic *record;

		if ((varname = read_string_with_null(fp)) == NULL)
			break;

		if ((record = find_option(varname, true, false, FATAL)) == NULL)
			elog(FATAL, "failed to locate variable \"%s\" in exec config params file", varname);

		if ((varvalue = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if ((varsourcefile = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsourceline, 1, sizeof(varsourceline), fp) != sizeof(varsourceline))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsource, 1, sizeof(varsource), fp) != sizeof(varsource))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varscontext, 1, sizeof(varscontext), fp) != sizeof(varscontext))
			elog(FATAL, "invalid format of exec config params file");

		(void) set_config_option(varname, varvalue,
								 varscontext, varsource,
								 GUC_ACTION_SET, true, 0, true);
		if (varsourcefile[0])
			set_config_sourcefile(varname, varsourcefile, varsourceline);

		free(varname);
		free(varvalue);
		free(varsourcefile);
	}

	FreeFile(fp);
}
#endif							/* EXEC_BACKEND */

/*
 * can_skip_gucvar:
 * Decide whether SerializeGUCState can skip sending this GUC variable,
 * or whether RestoreGUCState can skip resetting this GUC to default.
 *
 * It is somewhat magical and fragile that the same test works for both cases.
 * Realize in particular that we are very likely selecting different sets of
 * GUCs on the leader and worker sides!  Be sure you've understood the
 * comments here and in RestoreGUCState thoroughly before changing this.
 */
static bool
can_skip_gucvar(struct config_generic *gconf)
{
	/*
	 * We can skip GUCs that are guaranteed to have the same values in leaders
	 * and workers.  (Note it is critical that the leader and worker have the
	 * same idea of which GUCs fall into this category.  It's okay to consider
	 * context and name for this purpose, since those are unchanging
	 * properties of a GUC.)
	 *
	 * PGC_POSTMASTER variables always have the same value in every child of a
	 * particular postmaster, so the worker will certainly have the right
	 * value already.  Likewise, PGC_INTERNAL variables are set by special
	 * mechanisms (if indeed they aren't compile-time constants).  So we may
	 * always skip these.
	 *
	 * Role must be handled specially because its current value can be an
	 * invalid value (for instance, if someone dropped the role since we set
	 * it).  So if we tried to serialize it normally, we might get a failure.
	 * We skip it here, and use another mechanism to ensure the worker has the
	 * right value.
	 *
	 * For all other GUCs, we skip if the GUC has its compiled-in default
	 * value (i.e., source == PGC_S_DEFAULT).  On the leader side, this means
	 * we don't send GUCs that have their default values, which typically
	 * saves lots of work.  On the worker side, this means we don't need to
	 * reset the GUC to default because it already has that value.  See
	 * comments in RestoreGUCState for more info.
	 */
	return gconf->context == PGC_POSTMASTER ||
		gconf->context == PGC_INTERNAL || gconf->source == PGC_S_DEFAULT ||
		strcmp(gconf->name, "role") == 0;
}

/*
 * estimate_variable_size:
 *		Compute space needed for dumping the given GUC variable.
 *
 * It's OK to overestimate, but not to underestimate.
 */
static Size
estimate_variable_size(struct config_generic *gconf)
{
	Size		size;
	Size		valsize = 0;

	/* Skippable GUCs consume zero space. */
	if (can_skip_gucvar(gconf))
		return 0;

	/* Name, plus trailing zero byte. */
	size = strlen(gconf->name) + 1;

	/* Get the maximum display length of the GUC value. */
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				valsize = 5;	/* max(strlen('true'), strlen('false')) */
			}
			break;

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;

				/*
				 * Instead of getting the exact display length, use max
				 * length.  Also reduce the max length for typical ranges of
				 * small values.  Maximum value is 2147483647, i.e. 10 chars.
				 * Include one byte for sign.
				 */
				if (Abs(*conf->variable) < 1000)
					valsize = 3 + 1;
				else
					valsize = 10 + 1;
			}
			break;

		case PGC_REAL:
			{
				/*
				 * We are going to print it with %e with REALTYPE_PRECISION
				 * fractional digits.  Account for sign, leading digit,
				 * decimal point, and exponent with up to 3 digits.  E.g.
				 * -3.99329042340000021e+110
				 */
				valsize = 1 + 1 + 1 + REALTYPE_PRECISION + 5;
			}
			break;

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;

				/*
				 * If the value is NULL, we transmit it as an empty string.
				 * Although this is not physically the same value, GUC
				 * generally treats a NULL the same as empty string.
				 */
				if (*conf->variable)
					valsize = strlen(*conf->variable);
				else
					valsize = 0;
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;

				valsize = strlen(config_enum_lookup_by_value(conf, *conf->variable));
			}
			break;
	}

	/* Allow space for terminating zero-byte for value */
	size = add_size(size, valsize + 1);

	if (gconf->sourcefile)
		size = add_size(size, strlen(gconf->sourcefile));

	/* Allow space for terminating zero-byte for sourcefile */
	size = add_size(size, 1);

	/* Include line whenever file is nonempty. */
	if (gconf->sourcefile && gconf->sourcefile[0])
		size = add_size(size, sizeof(gconf->sourceline));

	size = add_size(size, sizeof(gconf->source));
	size = add_size(size, sizeof(gconf->scontext));

	return size;
}

/*
 * EstimateGUCStateSpace:
 * Returns the size needed to store the GUC state for the current process
 */
Size
EstimateGUCStateSpace(void)
{
	Size		size;
	int			i;

	/* Add space reqd for saving the data size of the guc state */
	size = sizeof(Size);

	/* Add up the space needed for each GUC variable */
	for (i = 0; i < num_guc_variables; i++)
		size = add_size(size,
						estimate_variable_size(guc_variables[i]));

	return size;
}

/*
 * do_serialize:
 * Copies the formatted string into the destination.  Moves ahead the
 * destination pointer, and decrements the maxbytes by that many bytes. If
 * maxbytes is not sufficient to copy the string, error out.
 */
static void
do_serialize(char **destptr, Size *maxbytes, const char *fmt,...)
{
	va_list		vargs;
	int			n;

	if (*maxbytes <= 0)
		elog(ERROR, "not enough space to serialize GUC state");

	va_start(vargs, fmt);
	n = vsnprintf(*destptr, *maxbytes, fmt, vargs);
	va_end(vargs);

	if (n < 0)
	{
		/* Shouldn't happen. Better show errno description. */
		elog(ERROR, "vsnprintf failed: %m with format string \"%s\"", fmt);
	}
	if (n >= *maxbytes)
	{
		/* This shouldn't happen either, really. */
		elog(ERROR, "not enough space to serialize GUC state");
	}

	/* Shift the destptr ahead of the null terminator */
	*destptr += n + 1;
	*maxbytes -= n + 1;
}

/* Binary copy version of do_serialize() */
static void
do_serialize_binary(char **destptr, Size *maxbytes, void *val, Size valsize)
{
	if (valsize > *maxbytes)
		elog(ERROR, "not enough space to serialize GUC state");

	memcpy(*destptr, val, valsize);
	*destptr += valsize;
	*maxbytes -= valsize;
}

/*
 * serialize_variable:
 * Dumps name, value and other information of a GUC variable into destptr.
 */
static void
serialize_variable(char **destptr, Size *maxbytes,
				   struct config_generic *gconf)
{
	/* Ignore skippable GUCs. */
	if (can_skip_gucvar(gconf))
		return;

	do_serialize(destptr, maxbytes, "%s", gconf->name);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) gconf;

				do_serialize(destptr, maxbytes,
							 (*conf->variable ? "true" : "false"));
			}
			break;

		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;

				do_serialize(destptr, maxbytes, "%d", *conf->variable);
			}
			break;

		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) gconf;

				do_serialize(destptr, maxbytes, "%.*e",
							 REALTYPE_PRECISION, *conf->variable);
			}
			break;

		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;

				/* NULL becomes empty string, see estimate_variable_size() */
				do_serialize(destptr, maxbytes, "%s",
							 *conf->variable ? *conf->variable : "");
			}
			break;

		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;

				do_serialize(destptr, maxbytes, "%s",
							 config_enum_lookup_by_value(conf, *conf->variable));
			}
			break;
	}

	do_serialize(destptr, maxbytes, "%s",
				 (gconf->sourcefile ? gconf->sourcefile : ""));

	if (gconf->sourcefile && gconf->sourcefile[0])
		do_serialize_binary(destptr, maxbytes, &gconf->sourceline,
							sizeof(gconf->sourceline));

	do_serialize_binary(destptr, maxbytes, &gconf->source,
						sizeof(gconf->source));
	do_serialize_binary(destptr, maxbytes, &gconf->scontext,
						sizeof(gconf->scontext));
}

/*
 * SerializeGUCState:
 * Dumps the complete GUC state onto the memory location at start_address.
 */
void
SerializeGUCState(Size maxsize, char *start_address)
{
	char	   *curptr;
	Size		actual_size;
	Size		bytes_left;
	int			i;

	/* Reserve space for saving the actual size of the guc state */
	Assert(maxsize > sizeof(actual_size));
	curptr = start_address + sizeof(actual_size);
	bytes_left = maxsize - sizeof(actual_size);

	for (i = 0; i < num_guc_variables; i++)
		serialize_variable(&curptr, &bytes_left, guc_variables[i]);

	/* Store actual size without assuming alignment of start_address. */
	actual_size = maxsize - bytes_left - sizeof(actual_size);
	memcpy(start_address, &actual_size, sizeof(actual_size));
}

/*
 * read_gucstate:
 * Actually it does not read anything, just returns the srcptr. But it does
 * move the srcptr past the terminating zero byte, so that the caller is ready
 * to read the next string.
 */
static char *
read_gucstate(char **srcptr, char *srcend)
{
	char	   *retptr = *srcptr;
	char	   *ptr;

	if (*srcptr >= srcend)
		elog(ERROR, "incomplete GUC state");

	/* The string variables are all null terminated */
	for (ptr = *srcptr; ptr < srcend && *ptr != '\0'; ptr++)
		;

	if (ptr >= srcend)
		elog(ERROR, "could not find null terminator in GUC state");

	/* Set the new position to the byte following the terminating NUL */
	*srcptr = ptr + 1;

	return retptr;
}

/* Binary read version of read_gucstate(). Copies into dest */
static void
read_gucstate_binary(char **srcptr, char *srcend, void *dest, Size size)
{
	if (*srcptr + size > srcend)
		elog(ERROR, "incomplete GUC state");

	memcpy(dest, *srcptr, size);
	*srcptr += size;
}

/*
 * Callback used to add a context message when reporting errors that occur
 * while trying to restore GUCs in parallel workers.
 */
static void
guc_restore_error_context_callback(void *arg)
{
	char	  **error_context_name_and_value = (char **) arg;

	if (error_context_name_and_value)
		errcontext("while setting parameter \"%s\" to \"%s\"",
				   error_context_name_and_value[0],
				   error_context_name_and_value[1]);
}

/*
 * RestoreGUCState:
 * Reads the GUC state at the specified address and sets this process's
 * GUCs to match.
 *
 * Note that this provides the worker with only a very shallow view of the
 * leader's GUC state: we'll know about the currently active values, but not
 * about stacked or reset values.  That's fine since the worker is just
 * executing one part of a query, within which the active values won't change
 * and the stacked values are invisible.
 */
void
RestoreGUCState(void *gucstate)
{
	char	   *varname,
			   *varvalue,
			   *varsourcefile;
	int			varsourceline;
	GucSource	varsource;
	GucContext	varscontext;
	char	   *srcptr = (char *) gucstate;
	char	   *srcend;
	Size		len;
	int			i;
	ErrorContextCallback error_context_callback;

	/*
	 * First, ensure that all potentially-shippable GUCs are reset to their
	 * default values.  We must not touch those GUCs that the leader will
	 * never ship, while there is no need to touch those that are shippable
	 * but already have their default values.  Thus, this ends up being the
	 * same test that SerializeGUCState uses, even though the sets of
	 * variables involved may well be different since the leader's set of
	 * variables-not-at-default-values can differ from the set that are
	 * not-default in this freshly started worker.
	 *
	 * Once we have set all the potentially-shippable GUCs to default values,
	 * restoring the GUCs that the leader sent (because they had non-default
	 * values over there) leads us to exactly the set of GUC values that the
	 * leader has.  This is true even though the worker may have initially
	 * absorbed postgresql.conf settings that the leader hasn't yet seen, or
	 * ALTER USER/DATABASE SET settings that were established after the leader
	 * started.
	 *
	 * Note that ensuring all the potential target GUCs are at PGC_S_DEFAULT
	 * also ensures that set_config_option won't refuse to set them because of
	 * source-priority comparisons.
	 */
	for (i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = guc_variables[i];

		/* Do nothing if non-shippable or if already at PGC_S_DEFAULT. */
		if (can_skip_gucvar(gconf))
			continue;

		/*
		 * We can use InitializeOneGUCOption to reset the GUC to default, but
		 * first we must free any existing subsidiary data to avoid leaking
		 * memory.  The stack must be empty, but we have to clean up all other
		 * fields.  Beware that there might be duplicate value or "extra"
		 * pointers.
		 */
		Assert(gconf->stack == NULL);
		if (gconf->extra)
			free(gconf->extra);
		if (gconf->last_reported)	/* probably can't happen */
			free(gconf->last_reported);
		if (gconf->sourcefile)
			free(gconf->sourcefile);
		switch (gconf->vartype)
		{
			case PGC_BOOL:
				{
					struct config_bool *conf = (struct config_bool *) gconf;

					if (conf->reset_extra && conf->reset_extra != gconf->extra)
						free(conf->reset_extra);
					break;
				}
			case PGC_INT:
				{
					struct config_int *conf = (struct config_int *) gconf;

					if (conf->reset_extra && conf->reset_extra != gconf->extra)
						free(conf->reset_extra);
					break;
				}
			case PGC_REAL:
				{
					struct config_real *conf = (struct config_real *) gconf;

					if (conf->reset_extra && conf->reset_extra != gconf->extra)
						free(conf->reset_extra);
					break;
				}
			case PGC_STRING:
				{
					struct config_string *conf = (struct config_string *) gconf;

					if (*conf->variable)
						free(*conf->variable);
					if (conf->reset_val && conf->reset_val != *conf->variable)
						free(conf->reset_val);
					if (conf->reset_extra && conf->reset_extra != gconf->extra)
						free(conf->reset_extra);
					break;
				}
			case PGC_ENUM:
				{
					struct config_enum *conf = (struct config_enum *) gconf;

					if (conf->reset_extra && conf->reset_extra != gconf->extra)
						free(conf->reset_extra);
					break;
				}
		}
		/* Now we can reset the struct to PGS_S_DEFAULT state. */
		InitializeOneGUCOption(gconf);
	}

	/* First item is the length of the subsequent data */
	memcpy(&len, gucstate, sizeof(len));

	srcptr += sizeof(len);
	srcend = srcptr + len;

	/* If the GUC value check fails, we want errors to show useful context. */
	error_context_callback.callback = guc_restore_error_context_callback;
	error_context_callback.previous = error_context_stack;
	error_context_callback.arg = NULL;
	error_context_stack = &error_context_callback;

	/* Restore all the listed GUCs. */
	while (srcptr < srcend)
	{
		int			result;
		char	   *error_context_name_and_value[2];

		varname = read_gucstate(&srcptr, srcend);
		varvalue = read_gucstate(&srcptr, srcend);
		varsourcefile = read_gucstate(&srcptr, srcend);
		if (varsourcefile[0])
			read_gucstate_binary(&srcptr, srcend,
								 &varsourceline, sizeof(varsourceline));
		else
			varsourceline = 0;
		read_gucstate_binary(&srcptr, srcend,
							 &varsource, sizeof(varsource));
		read_gucstate_binary(&srcptr, srcend,
							 &varscontext, sizeof(varscontext));

		error_context_name_and_value[0] = varname;
		error_context_name_and_value[1] = varvalue;
		error_context_callback.arg = &error_context_name_and_value[0];
		result = set_config_option(varname, varvalue, varscontext, varsource,
								   GUC_ACTION_SET, true, ERROR, true);
		if (result <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("parameter \"%s\" could not be set", varname)));
		if (varsourcefile[0])
			set_config_sourcefile(varname, varsourcefile, varsourceline);
		error_context_callback.arg = NULL;
	}

	error_context_stack = error_context_callback.previous;
}

/*
 * A little "long argument" simulation, although not quite GNU
 * compliant. Takes a string of the form "some-option=some value" and
 * returns name = "some_option" and value = "some value" in malloc'ed
 * storage. Note that '-' is converted to '_' in the option name. If
 * there is no '=' in the input string then value will be NULL.
 */
void
ParseLongOption(const char *string, char **name, char **value)
{
	size_t		equal_pos;
	char	   *cp;

	AssertArg(string);
	AssertArg(name);
	AssertArg(value);

	equal_pos = strcspn(string, "=");

	if (string[equal_pos] == '=')
	{
		*name = guc_malloc(FATAL, equal_pos + 1);
		strlcpy(*name, string, equal_pos + 1);

		*value = guc_strdup(FATAL, &string[equal_pos + 1]);
	}
	else
	{
		/* no equal sign in string */
		*name = guc_strdup(FATAL, string);
		*value = NULL;
	}

	for (cp = *name; *cp; cp++)
		if (*cp == '-')
			*cp = '_';
}


/*
 * Handle options fetched from pg_db_role_setting.setconfig,
 * pg_proc.proconfig, etc.  Caller must specify proper context/source/action.
 *
 * The array parameter must be an array of TEXT (it must not be NULL).
 */
void
ProcessGUCArray(ArrayType *array,
				GucContext context, GucSource source, GucAction action)
{
	int			i;

	Assert(array != NULL);
	Assert(ARR_ELEMTYPE(array) == TEXTOID);
	Assert(ARR_NDIM(array) == 1);
	Assert(ARR_LBOUND(array)[0] == 1);

	for (i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		bool		isnull;
		char	   *s;
		char	   *name;
		char	   *value;
		char	   *namecopy;
		char	   *valuecopy;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);

		if (isnull)
			continue;

		s = TextDatumGetCString(d);

		ParseLongOption(s, &name, &value);
		if (!value)
		{
			ereport(WARNING,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("could not parse setting for parameter \"%s\"",
							name)));
			free(name);
			continue;
		}

		/* free malloc'd strings immediately to avoid leak upon error */
		namecopy = pstrdup(name);
		free(name);
		valuecopy = pstrdup(value);
		free(value);

		(void) set_config_option(namecopy, valuecopy,
								 context, source,
								 action, true, 0, false);

		pfree(namecopy);
		pfree(valuecopy);
		pfree(s);
	}
}


/*
 * Add an entry to an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.
 */
ArrayType *
GUCArrayAdd(ArrayType *array, const char *name, const char *value)
{
	struct config_generic *record;
	Datum		datum;
	char	   *newval;
	ArrayType  *a;

	Assert(name);
	Assert(value);

	/* test if the option is valid and we're allowed to set it */
	(void) validate_option_array_item(name, value, false);

	/* normalize name (converts obsolete GUC names to modern spellings) */
	record = find_option(name, false, true, WARNING);
	if (record)
		name = record->name;

	/* build new item for array */
	newval = psprintf("%s=%s", name, value);
	datum = CStringGetTextDatum(newval);

	if (array)
	{
		int			index;
		bool		isnull;
		int			i;

		Assert(ARR_ELEMTYPE(array) == TEXTOID);
		Assert(ARR_NDIM(array) == 1);
		Assert(ARR_LBOUND(array)[0] == 1);

		index = ARR_DIMS(array)[0] + 1; /* add after end */

		for (i = 1; i <= ARR_DIMS(array)[0]; i++)
		{
			Datum		d;
			char	   *current;

			d = array_ref(array, 1, &i,
						  -1 /* varlenarray */ ,
						  -1 /* TEXT's typlen */ ,
						  false /* TEXT's typbyval */ ,
						  TYPALIGN_INT /* TEXT's typalign */ ,
						  &isnull);
			if (isnull)
				continue;
			current = TextDatumGetCString(d);

			/* check for match up through and including '=' */
			if (strncmp(current, newval, strlen(name) + 1) == 0)
			{
				index = i;
				break;
			}
		}

		a = array_set(array, 1, &index,
					  datum,
					  false,
					  -1 /* varlena array */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ );
	}
	else
		a = construct_array(&datum, 1,
							TEXTOID,
							-1, false, TYPALIGN_INT);

	return a;
}


/*
 * Delete an entry from an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.  Also, if the return value
 * is NULL then a null should be stored.
 */
ArrayType *
GUCArrayDelete(ArrayType *array, const char *name)
{
	struct config_generic *record;
	ArrayType  *newarray;
	int			i;
	int			index;

	Assert(name);

	/* test if the option is valid and we're allowed to set it */
	(void) validate_option_array_item(name, NULL, false);

	/* normalize name (converts obsolete GUC names to modern spellings) */
	record = find_option(name, false, true, WARNING);
	if (record)
		name = record->name;

	/* if array is currently null, then surely nothing to delete */
	if (!array)
		return NULL;

	newarray = NULL;
	index = 1;

	for (i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		char	   *val;
		bool		isnull;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);
		if (isnull)
			continue;
		val = TextDatumGetCString(d);

		/* ignore entry if it's what we want to delete */
		if (strncmp(val, name, strlen(name)) == 0
			&& val[strlen(name)] == '=')
			continue;

		/* else add it to the output array */
		if (newarray)
			newarray = array_set(newarray, 1, &index,
								 d,
								 false,
								 -1 /* varlenarray */ ,
								 -1 /* TEXT's typlen */ ,
								 false /* TEXT's typbyval */ ,
								 TYPALIGN_INT /* TEXT's typalign */ );
		else
			newarray = construct_array(&d, 1,
									   TEXTOID,
									   -1, false, TYPALIGN_INT);

		index++;
	}

	return newarray;
}


/*
 * Given a GUC array, delete all settings from it that our permission
 * level allows: if superuser, delete them all; if regular user, only
 * those that are PGC_USERSET
 */
ArrayType *
GUCArrayReset(ArrayType *array)
{
	ArrayType  *newarray;
	int			i;
	int			index;

	/* if array is currently null, nothing to do */
	if (!array)
		return NULL;

	/* if we're superuser, we can delete everything, so just do it */
	if (superuser())
		return NULL;

	newarray = NULL;
	index = 1;

	for (i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		char	   *val;
		char	   *eqsgn;
		bool		isnull;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);
		if (isnull)
			continue;
		val = TextDatumGetCString(d);

		eqsgn = strchr(val, '=');
		*eqsgn = '\0';

		/* skip if we have permission to delete it */
		if (validate_option_array_item(val, NULL, true))
			continue;

		/* else add it to the output array */
		if (newarray)
			newarray = array_set(newarray, 1, &index,
								 d,
								 false,
								 -1 /* varlenarray */ ,
								 -1 /* TEXT's typlen */ ,
								 false /* TEXT's typbyval */ ,
								 TYPALIGN_INT /* TEXT's typalign */ );
		else
			newarray = construct_array(&d, 1,
									   TEXTOID,
									   -1, false, TYPALIGN_INT);

		index++;
		pfree(val);
	}

	return newarray;
}

/*
 * Validate a proposed option setting for GUCArrayAdd/Delete/Reset.
 *
 * name is the option name.  value is the proposed value for the Add case,
 * or NULL for the Delete/Reset cases.  If skipIfNoPermissions is true, it's
 * not an error to have no permissions to set the option.
 *
 * Returns true if OK, false if skipIfNoPermissions is true and user does not
 * have permission to change this option (all other error cases result in an
 * error being thrown).
 */
static bool
validate_option_array_item(const char *name, const char *value,
						   bool skipIfNoPermissions)

{
	struct config_generic *gconf;

	/*
	 * There are three cases to consider:
	 *
	 * name is a known GUC variable.  Check the value normally, check
	 * permissions normally (i.e., allow if variable is USERSET, or if it's
	 * SUSET and user is superuser).
	 *
	 * name is not known, but exists or can be created as a placeholder (i.e.,
	 * it has a valid custom name).  We allow this case if you're a superuser,
	 * otherwise not.  Superusers are assumed to know what they're doing. We
	 * can't allow it for other users, because when the placeholder is
	 * resolved it might turn out to be a SUSET variable;
	 * define_custom_variable assumes we checked that.
	 *
	 * name is not known and can't be created as a placeholder.  Throw error,
	 * unless skipIfNoPermissions is true, in which case return false.
	 */
	gconf = find_option(name, true, skipIfNoPermissions, ERROR);
	if (!gconf)
	{
		/* not known, failed to make a placeholder */
		return false;
	}

	if (gconf->flags & GUC_CUSTOM_PLACEHOLDER)
	{
		/*
		 * We cannot do any meaningful check on the value, so only permissions
		 * are useful to check.
		 */
		if (superuser())
			return true;
		if (skipIfNoPermissions)
			return false;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set parameter \"%s\"", name)));
	}

	/* manual permissions check so we can avoid an error being thrown */
	if (gconf->context == PGC_USERSET)
		 /* ok */ ;
	else if (gconf->context == PGC_SUSET && superuser())
		 /* ok */ ;
	else if (skipIfNoPermissions)
		return false;
	/* if a permissions error should be thrown, let set_config_option do it */

	/* test for permissions and valid option value */
	(void) set_config_option(name, value,
							 superuser() ? PGC_SUSET : PGC_USERSET,
							 PGC_S_TEST, GUC_ACTION_SET, false, 0, false);

	return true;
}


/*
 * Called by check_hooks that want to override the normal
 * ERRCODE_INVALID_PARAMETER_VALUE SQLSTATE for check hook failures.
 *
 * Note that GUC_check_errmsg() etc are just macros that result in a direct
 * assignment to the associated variables.  That is ugly, but forced by the
 * limitations of C's macro mechanisms.
 */
void
GUC_check_errcode(int sqlerrcode)
{
	GUC_check_errcode_value = sqlerrcode;
}


/*
 * Convenience functions to manage calling a variable's check_hook.
 * These mostly take care of the protocol for letting check hooks supply
 * portions of the error report on failure.
 */

static bool
call_bool_check_hook(struct config_bool *conf, bool *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %d",
						conf->gen.name, (int) *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush any strings created in ErrorContext */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_int_check_hook(struct config_int *conf, int *newval, void **extra,
					GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %d",
						conf->gen.name, *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush any strings created in ErrorContext */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_real_check_hook(struct config_real *conf, double *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %g",
						conf->gen.name, *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush any strings created in ErrorContext */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_string_check_hook(struct config_string *conf, char **newval, void **extra,
					   GucSource source, int elevel)
{
	volatile bool result = true;

	/* Quick success if no hook */
	if (!conf->check_hook)
		return true;

	/*
	 * If elevel is ERROR, or if the check_hook itself throws an elog
	 * (undesirable, but not always avoidable), make sure we don't leak the
	 * already-malloc'd newval string.
	 */
	PG_TRY();
	{
		/* Reset variables that might be set by hook */
		GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
		GUC_check_errmsg_string = NULL;
		GUC_check_errdetail_string = NULL;
		GUC_check_errhint_string = NULL;

		if (!conf->check_hook(newval, extra, source))
		{
			ereport(elevel,
					(errcode(GUC_check_errcode_value),
					 GUC_check_errmsg_string ?
					 errmsg_internal("%s", GUC_check_errmsg_string) :
					 errmsg("invalid value for parameter \"%s\": \"%s\"",
							conf->gen.name, *newval ? *newval : ""),
					 GUC_check_errdetail_string ?
					 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
					 GUC_check_errhint_string ?
					 errhint("%s", GUC_check_errhint_string) : 0));
			/* Flush any strings created in ErrorContext */
			FlushErrorState();
			result = false;
		}
	}
	PG_CATCH();
	{
		free(*newval);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

static bool
call_enum_check_hook(struct config_enum *conf, int *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": \"%s\"",
						conf->gen.name,
						config_enum_lookup_by_value(conf, *newval)),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush any strings created in ErrorContext */
		FlushErrorState();
		return false;
	}

	return true;
}


/*
 * check_hook, assign_hook and show_hook subroutines
 */

static bool
check_wal_consistency_checking(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;
	bool		newwalconsistency[RM_MAX_ID + 1];

	/* Initialize the array */
	MemSet(newwalconsistency, 0, (RM_MAX_ID + 1) * sizeof(bool));

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(l, elemlist)
	{
		char	   *tok = (char *) lfirst(l);
		bool		found = false;
		RmgrId		rmid;

		/* Check for 'all'. */
		if (pg_strcasecmp(tok, "all") == 0)
		{
			for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
				if (RmgrTable[rmid].rm_mask != NULL)
					newwalconsistency[rmid] = true;
			found = true;
		}
		else
		{
			/*
			 * Check if the token matches with any individual resource
			 * manager.
			 */
			for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
			{
				if (pg_strcasecmp(tok, RmgrTable[rmid].rm_name) == 0 &&
					RmgrTable[rmid].rm_mask != NULL)
				{
					newwalconsistency[rmid] = true;
					found = true;
				}
			}
		}

		/* If a valid resource manager is found, check for the next one. */
		if (!found)
		{
			GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
			pfree(rawstring);
			list_free(elemlist);
			return false;
		}
	}

	pfree(rawstring);
	list_free(elemlist);

	/* assign new value */
	*extra = guc_malloc(ERROR, (RM_MAX_ID + 1) * sizeof(bool));
	memcpy(*extra, newwalconsistency, (RM_MAX_ID + 1) * sizeof(bool));
	return true;
}

static void
assign_wal_consistency_checking(const char *newval, void *extra)
{
	wal_consistency_checking = (bool *) extra;
}

static bool
check_log_destination(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;
	int			newlogdest = 0;
	int		   *myextra;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(l, elemlist)
	{
		char	   *tok = (char *) lfirst(l);

		if (pg_strcasecmp(tok, "stderr") == 0)
			newlogdest |= LOG_DESTINATION_STDERR;
		else if (pg_strcasecmp(tok, "csvlog") == 0)
			newlogdest |= LOG_DESTINATION_CSVLOG;
#ifdef HAVE_SYSLOG
		else if (pg_strcasecmp(tok, "syslog") == 0)
			newlogdest |= LOG_DESTINATION_SYSLOG;
#endif
#ifdef WIN32
		else if (pg_strcasecmp(tok, "eventlog") == 0)
			newlogdest |= LOG_DESTINATION_EVENTLOG;
#endif
		else
		{
			GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
			pfree(rawstring);
			list_free(elemlist);
			return false;
		}
	}

	pfree(rawstring);
	list_free(elemlist);

	myextra = (int *) guc_malloc(ERROR, sizeof(int));
	*myextra = newlogdest;
	*extra = (void *) myextra;

	return true;
}

static void
assign_log_destination(const char *newval, void *extra)
{
	Log_destination = *((int *) extra);
}

static void
assign_syslog_facility(int newval, void *extra)
{
#ifdef HAVE_SYSLOG
	set_syslog_parameters(syslog_ident_str ? syslog_ident_str : "postgres",
						  newval);
#endif
	/* Without syslog support, just ignore it */
}

static void
assign_syslog_ident(const char *newval, void *extra)
{
#ifdef HAVE_SYSLOG
	set_syslog_parameters(newval, syslog_facility);
#endif
	/* Without syslog support, it will always be set to "none", so ignore */
}


static void
assign_session_replication_role(int newval, void *extra)
{
	/*
	 * Must flush the plan cache when changing replication role; but don't
	 * flush unnecessarily.
	 */
	if (SessionReplicationRole != newval)
		ResetPlanCache();
}

static bool
check_temp_buffers(int *newval, void **extra, GucSource source)
{
	/*
	 * Once local buffers have been initialized, it's too late to change this.
	 * However, if this is only a test call, allow it.
	 */
	if (source != PGC_S_TEST && NLocBuffer && NLocBuffer != *newval)
	{
		GUC_check_errdetail("\"temp_buffers\" cannot be changed after any temporary tables have been accessed in the session.");
		return false;
	}
	return true;
}

static bool
check_bonjour(bool *newval, void **extra, GucSource source)
{
#ifndef USE_BONJOUR
	if (*newval)
	{
		GUC_check_errmsg("Bonjour is not supported by this build");
		return false;
	}
#endif
	return true;
}

static bool
check_ssl(bool *newval, void **extra, GucSource source)
{
#ifndef USE_SSL
	if (*newval)
	{
		GUC_check_errmsg("SSL is not supported by this build");
		return false;
	}
#endif
	return true;
}

static bool
check_stage_log_stats(bool *newval, void **extra, GucSource source)
{
	if (*newval && log_statement_stats)
	{
		GUC_check_errdetail("Cannot enable parameter when \"log_statement_stats\" is true.");
		return false;
	}
	return true;
}

static bool
check_log_stats(bool *newval, void **extra, GucSource source)
{
	if (*newval &&
		(log_parser_stats || log_planner_stats || log_executor_stats))
	{
		GUC_check_errdetail("Cannot enable \"log_statement_stats\" when "
							"\"log_parser_stats\", \"log_planner_stats\", "
							"or \"log_executor_stats\" is true.");
		return false;
	}
	return true;
}

static bool
check_canonical_path(char **newval, void **extra, GucSource source)
{
	/*
	 * Since canonicalize_path never enlarges the string, we can just modify
	 * newval in-place.  But watch out for NULL, which is the default value
	 * for external_pid_file.
	 */
	if (*newval)
		canonicalize_path(*newval);
	return true;
}

static bool
check_timezone_abbreviations(char **newval, void **extra, GucSource source)
{
	/*
	 * The boot_val given above for timezone_abbreviations is NULL. When we
	 * see this we just do nothing.  If this value isn't overridden from the
	 * config file then pg_timezone_abbrev_initialize() will eventually
	 * replace it with "Default".  This hack has two purposes: to avoid
	 * wasting cycles loading values that might soon be overridden from the
	 * config file, and to avoid trying to read the timezone abbrev files
	 * during InitializeGUCOptions().  The latter doesn't work in an
	 * EXEC_BACKEND subprocess because my_exec_path hasn't been set yet and so
	 * we can't locate PGSHAREDIR.
	 */
	if (*newval == NULL)
	{
		Assert(source == PGC_S_DEFAULT);
		return true;
	}

	/* OK, load the file and produce a malloc'd TimeZoneAbbrevTable */
	*extra = load_tzoffsets(*newval);

	/* tzparser.c returns NULL on failure, reporting via GUC_check_errmsg */
	if (!*extra)
		return false;

	return true;
}

static void
assign_timezone_abbreviations(const char *newval, void *extra)
{
	/* Do nothing for the boot_val default of NULL */
	if (!extra)
		return;

	InstallTimeZoneAbbrevs((TimeZoneAbbrevTable *) extra);
}

/*
 * pg_timezone_abbrev_initialize --- set default value if not done already
 *
 * This is called after initial loading of postgresql.conf.  If no
 * timezone_abbreviations setting was found therein, select default.
 * If a non-default value is already installed, nothing will happen.
 *
 * This can also be called from ProcessConfigFile to establish the default
 * value after a postgresql.conf entry for it is removed.
 */
static void
pg_timezone_abbrev_initialize(void)
{
	SetConfigOption("timezone_abbreviations", "Default",
					PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
}

static const char *
show_archive_command(void)
{
	if (XLogArchivingActive())
		return XLogArchiveCommand;
	else
		return "(disabled)";
}

static void
assign_tcp_keepalives_idle(int newval, void *extra)
{
	/*
	 * The kernel API provides no way to test a value without setting it; and
	 * once we set it we might fail to unset it.  So there seems little point
	 * in fully implementing the check-then-assign GUC API for these
	 * variables.  Instead we just do the assignment on demand.  pqcomm.c
	 * reports any problems via ereport(LOG).
	 *
	 * This approach means that the GUC value might have little to do with the
	 * actual kernel value, so we use a show_hook that retrieves the kernel
	 * value rather than trusting GUC's copy.
	 */
	(void) pq_setkeepalivesidle(newval, MyProcPort);
}

static const char *
show_tcp_keepalives_idle(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static __thread char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesidle(MyProcPort));
	return nbuf;
}

static void
assign_tcp_keepalives_interval(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivesinterval(newval, MyProcPort);
}

static const char *
show_tcp_keepalives_interval(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static __thread char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesinterval(MyProcPort));
	return nbuf;
}

static void
assign_tcp_keepalives_count(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivescount(newval, MyProcPort);
}

static const char *
show_tcp_keepalives_count(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static __thread char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivescount(MyProcPort));
	return nbuf;
}

static void
assign_tcp_user_timeout(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_settcpusertimeout(newval, MyProcPort);
}

static const char *
show_tcp_user_timeout(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static __thread char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_gettcpusertimeout(MyProcPort));
	return nbuf;
}

static bool
check_maxconnections(int *newval, void **extra, GucSource source)
{
	if (*newval + autovacuum_max_workers + 1 +
		max_worker_processes + max_wal_senders > MAX_BACKENDS)
		return false;
	return true;
}

static bool
check_autovacuum_max_workers(int *newval, void **extra, GucSource source)
{
	if (MaxConnections + *newval + 1 +
		max_worker_processes + max_wal_senders > MAX_BACKENDS)
		return false;
	return true;
}

static bool
check_max_wal_senders(int *newval, void **extra, GucSource source)
{
	if (MaxConnections + autovacuum_max_workers + 1 +
		max_worker_processes + *newval > MAX_BACKENDS)
		return false;
	return true;
}

static bool
check_autovacuum_work_mem(int *newval, void **extra, GucSource source)
{
	/*
	 * -1 indicates fallback.
	 *
	 * If we haven't yet changed the boot_val default of -1, just let it be.
	 * Autovacuum will look to maintenance_work_mem instead.
	 */
	if (*newval == -1)
		return true;

	/*
	 * We clamp manually-set values to at least 1MB.  Since
	 * maintenance_work_mem is always set to at least this value, do the same
	 * here.
	 */
	if (*newval < 1024)
		*newval = 1024;

	return true;
}

static bool
check_max_worker_processes(int *newval, void **extra, GucSource source)
{
	if (MaxConnections + autovacuum_max_workers + 1 +
		*newval + max_wal_senders > MAX_BACKENDS)
		return false;
	return true;
}

static bool
check_effective_io_concurrency(int *newval, void **extra, GucSource source)
{
#ifndef USE_PREFETCH
	if (*newval != 0)
	{
		GUC_check_errdetail("effective_io_concurrency must be set to 0 on platforms that lack posix_fadvise().");
		return false;
	}
#endif							/* USE_PREFETCH */
	return true;
}

static bool
check_maintenance_io_concurrency(int *newval, void **extra, GucSource source)
{
#ifndef USE_PREFETCH
	if (*newval != 0)
	{
		GUC_check_errdetail("maintenance_io_concurrency must be set to 0 on platforms that lack posix_fadvise().");
		return false;
	}
#endif							/* USE_PREFETCH */
	return true;
}

static bool
check_huge_page_size(int *newval, void **extra, GucSource source)
{
#if !(defined(MAP_HUGE_MASK) && defined(MAP_HUGE_SHIFT))
	/* Recent enough Linux only, for now.  See GetHugePageSize(). */
	if (*newval != 0)
	{
		GUC_check_errdetail("huge_page_size must be 0 on this platform.");
		return false;
	}
#endif
	return true;
}

static bool
check_client_connection_check_interval(int *newval, void **extra, GucSource source)
{
#ifndef POLLRDHUP
	/* Linux only, for now.  See pq_check_connection(). */
	if (*newval != 0)
	{
		GUC_check_errdetail("client_connection_check_interval must be set to 0 on platforms that lack POLLRDHUP.");
		return false;
	}
#endif
	return true;
}

static void
assign_pgstat_temp_directory(const char *newval, void *extra)
{
	/* check_canonical_path already canonicalized newval for us */
	char	   *dname;
	char	   *tname;
	char	   *fname;

	/* directory */
	dname = guc_malloc(ERROR, strlen(newval) + 1);	/* runtime dir */
	sprintf(dname, "%s", newval);

	/* global stats */
	tname = guc_malloc(ERROR, strlen(newval) + 12); /* /global.tmp */
	sprintf(tname, "%s/global.tmp", newval);
	fname = guc_malloc(ERROR, strlen(newval) + 13); /* /global.stat */
	sprintf(fname, "%s/global.stat", newval);

	if (pgstat_stat_directory)
		free(pgstat_stat_directory);
	pgstat_stat_directory = dname;
	if (pgstat_stat_tmpname)
		free(pgstat_stat_tmpname);
	pgstat_stat_tmpname = tname;
	if (pgstat_stat_filename)
		free(pgstat_stat_filename);
	pgstat_stat_filename = fname;
}

static bool
check_application_name(char **newval, void **extra, GucSource source)
{
	/* Only allow clean ASCII chars in the application name */
	pg_clean_ascii(*newval);

	return true;
}

static void
assign_application_name(const char *newval, void *extra)
{
	/* Update the pg_stat_activity view */
	pgstat_report_appname(newval);
}

static bool
check_cluster_name(char **newval, void **extra, GucSource source)
{
	/* Only allow clean ASCII chars in the cluster name */
	pg_clean_ascii(*newval);

	return true;
}

static const char *
show_unix_socket_permissions(void)
{
	static __thread char buf[12];

	snprintf(buf, sizeof(buf), "%04o", Unix_socket_permissions);
	return buf;
}

static const char *
show_log_file_mode(void)
{
	static __thread char buf[12];

	snprintf(buf, sizeof(buf), "%04o", Log_file_mode);
	return buf;
}

static const char *
show_data_directory_mode(void)
{
	static __thread char buf[12];

	snprintf(buf, sizeof(buf), "%04o", data_directory_mode);
	return buf;
}

static const char *
show_in_hot_standby(void)
{
	/*
	 * We display the actual state based on shared memory, so that this GUC
	 * reports up-to-date state if examined intra-query.  The underlying
	 * variable in_hot_standby changes only when we transmit a new value to
	 * the client.
	 */
	return RecoveryInProgress() ? "on" : "off";
}

/*
 * We split the input string, where commas separate function names
 * and certain whitespace chars are ignored, into a \0-separated (and
 * \0\0-terminated) list of function names.  This formulation allows
 * easy scanning when an error is thrown while avoiding the use of
 * non-reentrant strtok(), as well as keeping the output data in a
 * single palloc() chunk.
 */
static bool
check_backtrace_functions(char **newval, void **extra, GucSource source)
{
	int			newvallen = strlen(*newval);
	char	   *someval;
	int			validlen;
	int			i;
	int			j;

	/*
	 * Allow characters that can be C identifiers and commas as separators, as
	 * well as some whitespace for readability.
	 */
	validlen = strspn(*newval,
					  "0123456789_"
					  "abcdefghijklmnopqrstuvwxyz"
					  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
					  ", \n\t");
	if (validlen != newvallen)
	{
		GUC_check_errdetail("invalid character");
		return false;
	}

	if (*newval[0] == '\0')
	{
		*extra = NULL;
		return true;
	}

	/*
	 * Allocate space for the output and create the copy.  We could discount
	 * whitespace chars to save some memory, but it doesn't seem worth the
	 * trouble.
	 */
	someval = guc_malloc(ERROR, newvallen + 1 + 1);
	for (i = 0, j = 0; i < newvallen; i++)
	{
		if ((*newval)[i] == ',')
			someval[j++] = '\0';	/* next item */
		else if ((*newval)[i] == ' ' ||
				 (*newval)[i] == '\n' ||
				 (*newval)[i] == '\t')
			;					/* ignore these */
		else
			someval[j++] = (*newval)[i];	/* copy anything else */
	}

	/* two \0s end the setting */
	someval[j] = '\0';
	someval[j + 1] = '\0';

	*extra = someval;
	return true;
}

static void
assign_backtrace_functions(const char *newval, void *extra)
{
	backtrace_symbol_list = (char *) extra;
}

static bool
check_recovery_target_timeline(char **newval, void **extra, GucSource source)
{
	RecoveryTargetTimeLineGoal rttg;
	RecoveryTargetTimeLineGoal *myextra;

	if (strcmp(*newval, "current") == 0)
		rttg = RECOVERY_TARGET_TIMELINE_CONTROLFILE;
	else if (strcmp(*newval, "latest") == 0)
		rttg = RECOVERY_TARGET_TIMELINE_LATEST;
	else
	{
		rttg = RECOVERY_TARGET_TIMELINE_NUMERIC;

		errno = 0;
		strtoul(*newval, NULL, 0);
		if (errno == EINVAL || errno == ERANGE)
		{
			GUC_check_errdetail("recovery_target_timeline is not a valid number.");
			return false;
		}
	}

	myextra = (RecoveryTargetTimeLineGoal *) guc_malloc(ERROR, sizeof(RecoveryTargetTimeLineGoal));
	*myextra = rttg;
	*extra = (void *) myextra;

	return true;
}

static void
assign_recovery_target_timeline(const char *newval, void *extra)
{
	recoveryTargetTimeLineGoal = *((RecoveryTargetTimeLineGoal *) extra);
	if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC)
		recoveryTargetTLIRequested = (TimeLineID) strtoul(newval, NULL, 0);
	else
		recoveryTargetTLIRequested = 0;
}

/*
 * Recovery target settings: Only one of the several recovery_target* settings
 * may be set.  Setting a second one results in an error.  The global variable
 * recoveryTarget tracks which kind of recovery target was chosen.  Other
 * variables store the actual target value (for example a string or a xid).
 * The assign functions of the parameters check whether a competing parameter
 * was already set.  But we want to allow setting the same parameter multiple
 * times.  We also want to allow unsetting a parameter and setting a different
 * one, so we unset recoveryTarget when the parameter is set to an empty
 * string.
 */

static void
pg_attribute_noreturn()
error_multiple_recovery_targets(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("multiple recovery targets specified"),
			 errdetail("At most one of recovery_target, recovery_target_lsn, recovery_target_name, recovery_target_time, recovery_target_xid may be set.")));
}

static bool
check_recovery_target(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "immediate") != 0 && strcmp(*newval, "") != 0)
	{
		GUC_check_errdetail("The only allowed value is \"immediate\".");
		return false;
	}
	return true;
}

static void
assign_recovery_target(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_IMMEDIATE)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
		recoveryTarget = RECOVERY_TARGET_IMMEDIATE;
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

static bool
check_recovery_target_xid(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		TransactionId xid;
		TransactionId *myextra;

		errno = 0;
		xid = (TransactionId) pg_strtouint64(*newval, NULL, 0);
		if (errno == EINVAL || errno == ERANGE)
			return false;

		myextra = (TransactionId *) guc_malloc(ERROR, sizeof(TransactionId));
		*myextra = xid;
		*extra = (void *) myextra;
	}
	return true;
}

static void
assign_recovery_target_xid(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_XID)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_XID;
		recoveryTargetXid = *((TransactionId *) extra);
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

/*
 * The interpretation of the recovery_target_time string can depend on the
 * time zone setting, so we need to wait until after all GUC processing is
 * done before we can do the final parsing of the string.  This check function
 * only does a parsing pass to catch syntax errors, but we store the string
 * and parse it again when we need to use it.
 */
static bool
check_recovery_target_time(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		/* reject some special values */
		if (strcmp(*newval, "now") == 0 ||
			strcmp(*newval, "today") == 0 ||
			strcmp(*newval, "tomorrow") == 0 ||
			strcmp(*newval, "yesterday") == 0)
		{
			return false;
		}

		/*
		 * parse timestamp value (see also timestamptz_in())
		 */
		{
			char	   *str = *newval;
			fsec_t		fsec;
			struct pg_tm tt,
					   *tm = &tt;
			int			tz;
			int			dtype;
			int			nf;
			int			dterr;
			char	   *field[MAXDATEFIELDS];
			int			ftype[MAXDATEFIELDS];
			char		workbuf[MAXDATELEN + MAXDATEFIELDS];
			TimestampTz timestamp;

			dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
								  field, ftype, MAXDATEFIELDS, &nf);
			if (dterr == 0)
				dterr = DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tz);
			if (dterr != 0)
				return false;
			if (dtype != DTK_DATE)
				return false;

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
			{
				GUC_check_errdetail("timestamp out of range: \"%s\"", str);
				return false;
			}
		}
	}
	return true;
}

static void
assign_recovery_target_time(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_TIME)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
		recoveryTarget = RECOVERY_TARGET_TIME;
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

static bool
check_recovery_target_name(char **newval, void **extra, GucSource source)
{
	/* Use the value of newval directly */
	if (strlen(*newval) >= MAXFNAMELEN)
	{
		GUC_check_errdetail("%s is too long (maximum %d characters).",
							"recovery_target_name", MAXFNAMELEN - 1);
		return false;
	}
	return true;
}

static void
assign_recovery_target_name(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_NAME)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_NAME;
		recoveryTargetName = newval;
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

static bool
check_recovery_target_lsn(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		XLogRecPtr	lsn;
		XLogRecPtr *myextra;
		bool		have_error = false;

		lsn = pg_lsn_in_internal(*newval, &have_error);
		if (have_error)
			return false;

		myextra = (XLogRecPtr *) guc_malloc(ERROR, sizeof(XLogRecPtr));
		*myextra = lsn;
		*extra = (void *) myextra;
	}
	return true;
}

static void
assign_recovery_target_lsn(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_LSN)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_LSN;
		recoveryTargetLSN = *((XLogRecPtr *) extra);
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

static bool
check_primary_slot_name(char **newval, void **extra, GucSource source)
{
	if (*newval && strcmp(*newval, "") != 0 &&
		!ReplicationSlotValidateName(*newval, WARNING))
		return false;

	return true;
}

static bool
check_default_with_oids(bool *newval, void **extra, GucSource source)
{
	if (*newval)
	{
		/* check the GUC's definition for an explanation */
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errmsg("tables declared WITH OIDS are not supported");

		return false;
	}

	return true;
}

#include "guc-file.c"
