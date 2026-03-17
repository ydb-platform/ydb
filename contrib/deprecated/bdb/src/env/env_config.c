/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/db_page.h"
#include "dbinc_auto/db_ext.h"

/*
 * DB_CONFIG lines are processed primarily by interpreting the command
 * description tables initialized below.
 *
 * Most DB_CONFIG commands consist of a single token name followed by one or two
 * integer or string arguments. These commands are described by entries in the
 * config_descs[] array.
 *
 * The remaining, usually more complex, DB_CONFIG commands are handled by small
 * code blocks in __config_parse().  Many of those commands need to translate
 * option names to the integer values needed by the API configuration functions.
 * Below the __config_descs[] initialization there are many FN array
 * initializations which provide the mapping between user-specifiable strings
 * and internally-used integer values. Typically there is one of these mappings
 * defined for each complex DB_CONFIG command. Use __db_name_to_val()
 * to translate a string to its integer value.
 */
typedef enum {
	CFG_INT,	/* The argument is 1 signed integer. */
	CFG_LONG,	/* The argument is 1 signed long int. */
	CFG_UINT,	/* The argument is 1 unsigned integer. */
	CFG_2INT,	/* The arguments are 2 signed integers. */
	CFG_2UINT,	/* The arguments are 2 unsigned integers. */
	CFG_STRING	/* The rest of the line is a string. */
} __db_config_type;

typedef struct __db_config_desc {
	char *name;		/* The name of a simple DB_CONFIG command. */
	__db_config_type type;	/* The enum describing its argument type(s). */
	int (*func)();		/* The function to call with the argument(s). */
} CFG_DESC;

/* These typedefs help eliminate lint warnings where "func" above is used. */
typedef int (*CFG_FUNC_STRING) __P((DB_ENV *, const char *));
typedef int (*CFG_FUNC_INT) __P((DB_ENV *, int));
typedef int (*CFG_FUNC_LONG) __P((DB_ENV *, long));
typedef int (*CFG_FUNC_UINT) __P((DB_ENV *, u_int32_t));
typedef int (*CFG_FUNC_2INT) __P((DB_ENV *, int, int));
typedef int (*CFG_FUNC_2UINT) __P((DB_ENV *, u_int32_t, u_int32_t));

/*
 * This table lists the simple DB_CONFIG configuration commands. It is sorted by
 * the command name, so that __config_scan() can bsearch() it.  After making an
 * addition to this table, please be sure that it remains sorted.  With vi or
 * vim, the following command line will do it:
 *	:/^static const CFG_DESC config_descs/+1, /^}/-1 ! sort
 *
 * This table can contain aliases.  Aliases have different names with identical
 * types and functions. At this time there are four aliases:
 *	Outdated Name		Current Name
 *	db_data_dir		set_data_dir
 *	db_log_dir		set_lg_dir
 *	db_tmp_dir		set_tmp_dir
 *	set_tas_spins		mutex_set_tas_spins
 */
static const CFG_DESC config_descs[] = {
    { "add_data_dir",		CFG_STRING,	__env_add_data_dir	},
    { "db_data_dir",		CFG_STRING,	__env_set_data_dir	},
    { "db_log_dir",		CFG_STRING,	__log_set_lg_dir	},
    { "db_tmp_dir",		CFG_STRING,	__env_set_tmp_dir	},
    { "mutex_set_align",	CFG_UINT,	__mutex_set_align	},
    { "mutex_set_increment",	CFG_UINT,	__mutex_set_increment	},
    { "mutex_set_init",		CFG_UINT,	__mutex_set_init	},
    { "mutex_set_max",		CFG_UINT,	__mutex_set_max		},
    { "mutex_set_tas_spins",	CFG_UINT,	__mutex_set_tas_spins	},
    { "rep_set_clockskew",	CFG_2UINT,	__rep_set_clockskew	},
    { "rep_set_limit",		CFG_2UINT,	__rep_set_limit		},
    { "rep_set_nsites",		CFG_UINT,	__rep_set_nsites_pp	},
    { "rep_set_priority",	CFG_UINT,	__rep_set_priority	},
    { "rep_set_request",	CFG_2UINT,	__rep_set_request	},
    { "set_cache_max",		CFG_2UINT,	__memp_set_cache_max	},
    { "set_create_dir",		CFG_STRING,	__env_set_create_dir	},
    { "set_data_dir",		CFG_STRING,	__env_set_data_dir	},
    { "set_data_len",		CFG_UINT,	__env_set_data_len	},
    { "set_intermediate_dir_mode",CFG_STRING, __env_set_intermediate_dir_mode },
    { "set_lg_bsize",		CFG_UINT,	__log_set_lg_bsize	},
    { "set_lg_dir",		CFG_STRING,	__log_set_lg_dir	},
    { "set_lg_filemode",	CFG_INT,	__log_set_lg_filemode	},
    { "set_lg_max",		CFG_UINT,	__log_set_lg_max	},
    { "set_lg_regionmax",	CFG_UINT,	__log_set_lg_regionmax	},
    { "set_lk_max_lockers",	CFG_UINT,	__lock_set_lk_max_lockers },
    { "set_lk_max_locks",	CFG_UINT,	__lock_set_lk_max_locks },
    { "set_lk_max_objects",	CFG_UINT,	__lock_set_lk_max_objects },
    { "set_lk_partitions",	CFG_UINT,	__lock_set_lk_partitions },
    { "set_lk_tablesize",	CFG_UINT,	__lock_set_lk_tablesize },
    { "set_memory_max",		CFG_2UINT,	__env_set_memory_max	},
    { "set_metadata_dir",	CFG_STRING,	__env_set_metadata_dir	},
    { "set_mp_max_openfd",	CFG_INT,	__memp_set_mp_max_openfd },
    { "set_mp_max_write",	CFG_2INT,	__memp_set_mp_max_write },
    { "set_mp_mmapsize",	CFG_UINT,	__memp_set_mp_mmapsize	},
    { "set_mp_mtxcount",	CFG_UINT,	__memp_set_mp_mtxcount	},
    { "set_mp_pagesize",	CFG_UINT,	__memp_set_mp_pagesize	},
    { "set_shm_key",		CFG_LONG,	__env_set_shm_key	},
    { "set_tas_spins",		CFG_UINT,	__mutex_set_tas_spins	},
    { "set_thread_count",	CFG_UINT,	__env_set_thread_count },
    { "set_tmp_dir",		CFG_STRING,	__env_set_tmp_dir	},
    { "set_tx_max",		CFG_UINT,	__txn_set_tx_max	}
};

/*
 * Here are the option-name to option-value mappings used by complex commands.
 */
static const FN config_mem_init[] = {
	{ (u_int32_t) DB_MEM_LOCK,		"DB_MEM_LOCK" },
	{ (u_int32_t) DB_MEM_LOCKER,		"DB_MEM_LOCKER" },
	{ (u_int32_t) DB_MEM_LOCKOBJECT,	"DB_MEM_LOCKOBJECT" },
	{ (u_int32_t) DB_MEM_TRANSACTION,	"DB_MEM_TRANSACTION" },
	{ (u_int32_t) DB_MEM_THREAD,		"DB_MEM_THREAD" },
	{ (u_int32_t) DB_MEM_LOGID,		"DB_MEM_LOGID" },
	{ 0, NULL }
};

static const FN config_rep_config[] = {
	{ DB_REP_CONF_AUTOINIT,		"db_rep_conf_autoinit" },
	{ DB_REP_CONF_AUTOROLLBACK,	"db_rep_conf_autorollback" },
	{ DB_REP_CONF_BULK,		"db_rep_conf_bulk" },
	{ DB_REP_CONF_DELAYCLIENT,	"db_rep_conf_delayclient" },
	{ DB_REP_CONF_INMEM,		"db_rep_conf_inmem" },
	{ DB_REP_CONF_LEASE,		"db_rep_conf_lease" },
	{ DB_REP_CONF_NOWAIT,		"db_rep_conf_nowait" },
	{ DB_REPMGR_CONF_2SITE_STRICT,	"db_repmgr_conf_2site_strict" },
	{ DB_REPMGR_CONF_ELECTIONS,	"db_repmgr_conf_elections" },
	{ 0, NULL }
};

static const FN config_rep_timeout[] = {
	{ DB_REP_ACK_TIMEOUT,		"db_rep_ack_timeout" },
	{ DB_REP_CHECKPOINT_DELAY,	"db_rep_checkpoint_delay" },
	{ DB_REP_CONNECTION_RETRY,	"db_rep_connection_retry" },
	{ DB_REP_ELECTION_TIMEOUT,	"db_rep_election_timeout" },
	{ DB_REP_ELECTION_RETRY,	"db_rep_election_retry" },
	{ DB_REP_FULL_ELECTION_TIMEOUT,	"db_rep_full_election_timeout" },
	{ DB_REP_HEARTBEAT_MONITOR,	"db_rep_heartbeat_monitor" },
	{ DB_REP_HEARTBEAT_SEND,	"db_rep_heartbeat_send" },
	{ DB_REP_LEASE_TIMEOUT,		"db_rep_lease_timeout" },
	{ 0, NULL }
};

static const FN config_repmgr_ack_policy[] = {
	{ DB_REPMGR_ACKS_ALL,		"db_repmgr_acks_all" },
	{ DB_REPMGR_ACKS_ALL_AVAILABLE,	"db_repmgr_acks_all_available" },
	{ DB_REPMGR_ACKS_ALL_PEERS,	"db_repmgr_acks_all_peers" },
	{ DB_REPMGR_ACKS_NONE,		"db_repmgr_acks_none" },
	{ DB_REPMGR_ACKS_ONE,		"db_repmgr_acks_one" },
	{ DB_REPMGR_ACKS_ONE_PEER,	"db_repmgr_acks_one_peer" },
	{ DB_REPMGR_ACKS_QUORUM,	"db_repmgr_acks_quorum" },
	{ 0, NULL }
};

static const FN config_repmgr_site[] = {
	{ DB_BOOTSTRAP_HELPER,	"db_bootstrap_helper" },
	{ DB_GROUP_CREATOR,	"db_group_creator" },
	{ DB_LEGACY,		"db_legacy" },
	{ DB_LOCAL_SITE,	"db_local_site" },
	{ DB_REPMGR_PEER,	"db_repmgr_peer" },
	{ 0, NULL }
};

static const FN config_set_flags[] = {
	{ DB_AUTO_COMMIT,	"db_auto_commit" },
	{ DB_CDB_ALLDB,		"db_cdb_alldb" },
	{ DB_DIRECT_DB,		"db_direct_db" },
	{ DB_DSYNC_DB,		"db_dsync_db" },
	{ DB_MULTIVERSION,	"db_multiversion" },
	{ DB_NOLOCKING,		"db_nolocking" },
	{ DB_NOMMAP,		"db_nommap" },
	{ DB_NOPANIC,		"db_nopanic" },
	{ DB_OVERWRITE,		"db_overwrite" },
	{ DB_REGION_INIT,	"db_region_init" },
	{ DB_TIME_NOTGRANTED,	"db_time_notgranted" },
	{ DB_TXN_NOSYNC,	"db_txn_nosync" },
	{ DB_TXN_NOWAIT,	"db_txn_nowait" },
	{ DB_TXN_SNAPSHOT,	"db_txn_snapshot" },
	{ DB_TXN_WRITE_NOSYNC,	"db_txn_write_nosync" },
	{ DB_YIELDCPU,		"db_yieldcpu" },
	{ 0, NULL }
};

static const FN config_set_flags_forlog[] = {
	{ DB_LOG_DIRECT,	"db_direct_log" },
	{ DB_LOG_DSYNC,		"db_dsync_log" },
	{ DB_LOG_AUTO_REMOVE,	"db_log_autoremove" },
	{ DB_LOG_IN_MEMORY,	"db_log_inmemory" },
	{ 0, NULL }
};

static const FN config_log_set_config[] = {
	{ DB_LOG_DIRECT,	"db_log_direct" },
	{ DB_LOG_DSYNC,		"db_log_dsync" },
	{ DB_LOG_AUTO_REMOVE,	"db_log_auto_remove" },
	{ DB_LOG_IN_MEMORY,	"db_log_in_memory" },
	{ DB_LOG_ZERO,		"db_log_zero" },
	{ 0, NULL }
};

static const FN config_set_lk_detect[] = {
	{ DB_LOCK_DEFAULT,	"db_lock_default" },
	{ DB_LOCK_EXPIRE,	"db_lock_expire" },
	{ DB_LOCK_MAXLOCKS,	"db_lock_maxlocks" },
	{ DB_LOCK_MAXWRITE,	"db_lock_maxwrite" },
	{ DB_LOCK_MINLOCKS,	"db_lock_minlocks" },
	{ DB_LOCK_MINWRITE,	"db_lock_minwrite" },
	{ DB_LOCK_OLDEST,	"db_lock_oldest" },
	{ DB_LOCK_RANDOM,	"db_lock_random" },
	{ DB_LOCK_YOUNGEST,	"db_lock_youngest" },
	{ 0, NULL }
};

static const FN config_set_open_flags[] = {
	{ DB_INIT_REP,	"db_init_rep" },
	{ DB_PRIVATE,	"db_private" },
	{ DB_REGISTER,	"db_register" },
	{ DB_THREAD,	"db_thread" },
	{ 0, NULL }
};

static const FN config_set_verbose[] = {
	{ DB_VERB_BACKUP,	"db_verb_backup" },
	{ DB_VERB_DEADLOCK,	"db_verb_deadlock" },
	{ DB_VERB_FILEOPS,	"db_verb_fileops" },
	{ DB_VERB_FILEOPS_ALL,	"db_verb_fileops_all" },
	{ DB_VERB_RECOVERY,	"db_verb_recovery" },
	{ DB_VERB_REGISTER,	"db_verb_register" },
	{ DB_VERB_REPLICATION,	"db_verb_replication" },
	{ DB_VERB_REP_ELECT,	"db_verb_rep_elect" },
	{ DB_VERB_REP_LEASE,	"db_verb_rep_lease" },
	{ DB_VERB_REP_MISC,	"db_verb_rep_misc" },
	{ DB_VERB_REP_MSGS,	"db_verb_rep_msgs" },
	{ DB_VERB_REP_SYNC,	"db_verb_rep_sync" },
	{ DB_VERB_REP_SYSTEM,	"db_verb_rep_system" },
	{ DB_VERB_REP_TEST,	"db_verb_rep_test" },
	{ DB_VERB_REPMGR_CONNFAIL,	"db_verb_repmgr_connfail" },
	{ DB_VERB_REPMGR_MISC,	"db_verb_repmgr_misc" },
	{ DB_VERB_WAITSFOR,	"db_verb_waitsfor" },
	{ 0, NULL}
};

static int __config_parse __P((ENV *, char *, int));
static int __config_scan __P((char *, char **, const CFG_DESC **));
static int cmp_cfg_name __P((const void *, const void *element));

/*
 * __env_read_db_config --
 *	Read the DB_CONFIG file.
 *
 * PUBLIC: int __env_read_db_config __P((ENV *));
 */
int
__env_read_db_config(env)
	ENV *env;
{
	FILE *fp;
	int lc, ret;
	char *p, buf[256];

	/* Parse the config file. */
	p = NULL;
	if ((ret = __db_appname(env,
	    DB_APP_NONE, "DB_CONFIG", NULL, &p)) != 0)
		return (ret);
	if (p == NULL)
		fp = NULL;
	else {
		fp = fopen(p, "r");
		__os_free(env, p);
	}

	if (fp == NULL)
		return (0);

	for (lc = 1; fgets(buf, sizeof(buf), fp) != NULL; ++lc) {
		if ((p = strchr(buf, '\n')) == NULL)
			p = buf + strlen(buf);
		if (p > buf && p[-1] == '\r')
			--p;
		*p = '\0';
		for (p = buf; *p != '\0' && isspace((int)*p); ++p)
			;
		if (*p == '\0' || *p == '#')
			continue;

		if ((ret = __config_parse(env, p, lc)) != 0)
			break;
	}
	(void)fclose(fp);

	return (ret);
}

#undef	CFG_GET_INT
#define	CFG_GET_INT(s, vp) do {					\
	int __ret;							\
	if ((__ret =							\
	    __db_getlong(env->dbenv, NULL, s, 0, INT_MAX, vp)) != 0)	\
		return (__ret);						\
} while (0)
#undef	CFG_GET_LONG
#define	CFG_GET_LONG(s, vp) do {					\
	int __ret;							\
	if ((__ret =							\
	    __db_getlong(env->dbenv, NULL, s, 0, LONG_MAX, vp)) != 0)	\
		return (__ret);						\
} while (0)
#undef	CFG_GET_UINT
#define	CFG_GET_UINT(s, vp) do {					\
	int __ret;							\
	if ((__ret =							\
	    __db_getulong(env->dbenv, NULL, s, 0, UINT_MAX, vp)) != 0)	\
		return (__ret);						\
} while (0)
#undef	CFG_GET_UINT32
#define	CFG_GET_UINT32(s, vp) do {					\
	if (__db_getulong(env->dbenv, NULL, s, 0, UINT32_MAX, vp) != 0)	\
		return (EINVAL);					\
} while (0)

/* This is the maximum number of tokens in a DB_CONFIG line. */
#undef	CFG_SLOTS
#define	CFG_SLOTS	10

/*
 * __config_parse --
 *	Parse a single NAME VALUE pair.
 */
static int
__config_parse(env, s, lc)
	ENV *env;
	char *s;
	int lc;
{
	DB_ENV *dbenv;
	DB_SITE *site;
	u_long uv1, uv2;
	long lv1, lv2;
	u_int port;
	int i, nf, onoff, bad, ret, t_ret;
	char *argv[CFG_SLOTS];
	const CFG_DESC *desc;

	bad = 0;
	dbenv = env->dbenv;

	/*
	 * Split the input line in 's' into its argv-like components, returning
	 * the number of fields. If the command is one of the "simple" ones in
	 * config_descs, also return its command descriptor.
	 */
	if ((nf = __config_scan(s, argv, &desc)) < 2) {
format:		__db_errx(env, DB_STR_A("1584",
		    "line %d: %s: incorrect name-value pair", "%d %s"),
			    lc, argv[0]);
		return (EINVAL);
	}

	/* Handle simple configuration lines here. */
	if (desc != NULL) {
		ret = 0;
		switch (desc->type) {
		  case CFG_INT:		/* <command> <int> */
			if (nf != 2)
				goto format;
			CFG_GET_INT(argv[1], &lv1);
			ret = ((CFG_FUNC_INT)desc->func)(dbenv, (int) lv1);
			break;

		  case CFG_LONG:	/* <command> <long int> */
			if (nf != 2)
				goto format;
			CFG_GET_LONG(argv[1], &lv1);
			ret = ((CFG_FUNC_LONG)desc->func)(dbenv, lv1);
			break;

		  case CFG_UINT:	/* <command> <uint> */
			if (nf != 2)
				goto format;
			CFG_GET_UINT(argv[1], &uv1);
			ret = ((CFG_FUNC_UINT)desc->func)
			    (dbenv, (u_int32_t) uv1);
			break;

		  case CFG_2INT:	/* <command> <int1> <int2> */
			if (nf != 3)
				goto format;
			CFG_GET_INT(argv[1], &lv1);
			CFG_GET_INT(argv[2], &lv2);
			ret = ((CFG_FUNC_2INT)desc->func)
			    (dbenv, (int) lv1, (int) lv2);
			break;

		  case CFG_2UINT:	/* <command> <uint1> <uint2> */
			if (nf != 3)
				goto format;
			CFG_GET_UINT(argv[1], &uv1);
			CFG_GET_UINT(argv[2], &uv2);
			ret = ((CFG_FUNC_2UINT)desc->func)
			    (dbenv, (u_int32_t) uv1, (u_int32_t) uv2);
			break;

		  case CFG_STRING:	/* <command> <rest of line as string> */
			ret = ((CFG_FUNC_STRING) desc->func)(dbenv, argv[1]);
			break;
		}
		return (ret);
	}

	/*
	 * The commands not covered in config_descs are handled below, each
	 * with their own command-specific block of code. Most of them are
	 * fairly similar to each other, but not quite enough to warrant
	 * that they all be table-driven too.
	 */

	/* set_memory_init db_mem_XXX <unsigned> */
	if (strcasecmp(argv[0], "set_memory_init") == 0) {
		if (nf != 3)
			goto format;
		if ((lv1 = __db_name_to_val(config_mem_init, argv[1])) == -1)
			goto format;
		CFG_GET_UINT32(argv[2], &uv2);
		return (__env_set_memory_init(dbenv,
		    (DB_MEM_CONFIG) lv1, (u_int32_t)uv2));
	}

	/* rep_set_config { db_rep_conf_XXX | db_repmgr_conf_XXX }  [on|off] */
	if (strcasecmp(argv[0], "rep_set_config") == 0) {
		if (nf != 2 && nf != 3)
			goto format;
		onoff = 1;
		if (nf == 3) {
			if (strcasecmp(argv[2], "off") == 0)
				onoff = 0;
			else if (strcasecmp(argv[2], "on") != 0)
				goto format;
		}
		if ((lv1 = __db_name_to_val(config_rep_config, argv[1])) == -1)
			goto format;
		return (__rep_set_config(dbenv, (u_int32_t)lv1, onoff));
	}

	/* rep_set_timeout db_rep_XXX <unsigned> */
	if (strcasecmp(argv[0], "rep_set_timeout") == 0) {
		if (nf != 3)
			goto format;
		if ((lv1 = __db_name_to_val(config_rep_timeout, argv[1])) == -1)
			goto format;
		CFG_GET_UINT32(argv[2], &uv2);
		return (__rep_set_timeout(dbenv, lv1, (db_timeout_t)uv2));
	}

	/* repmgr_set_ack_policy db_repmgr_acks_XXX */
	if (strcasecmp(argv[0], "repmgr_set_ack_policy") == 0) {
		if (nf != 2)
			goto format;
		if ((lv1 =
		    __db_name_to_val(config_repmgr_ack_policy, argv[1])) == -1)
			goto format;
		return (__repmgr_set_ack_policy(dbenv, lv1));
	}

	/*
	 * Configure name/value pairs of config information for a site (local or
	 * remote).
	 *
	 * repmgr_site host port [which value(on | off | unsigned)}] ...
	 */
	if (strcasecmp(argv[0], "repmgr_site") == 0) {
		if (nf < 3 || (nf % 2) == 0)
			goto format;
		CFG_GET_UINT(argv[2], &uv2);
		port = (u_int)uv2;

		if ((ret = __repmgr_site(dbenv, argv[1], port, &site, 0)) != 0)
			return (ret);
#ifdef HAVE_REPLICATION_THREADS
		for (i = 3; i < nf; i += 2) {
			if ((lv1 = __db_name_to_val(
			    config_repmgr_site, argv[i])) == -1) {
				bad = 1;
				break;
			}

			if (strcasecmp(argv[i + 1], "on") == 0)
				uv2 = 1;
			else if (strcasecmp(argv[i + 1], "off") == 0)
				uv2 = 0;
			else
				CFG_GET_UINT32(argv[i + 1], &uv2);
			if ((ret = __repmgr_site_config(site,
			    (u_int32_t)lv1, (u_int32_t)uv2)) != 0)
				break;
		}
		if ((t_ret = __repmgr_site_close(site)) != 0 && ret == 0)
			ret = t_ret;
		if (bad)
			goto format;
#else
		/* If repmgr not built, __repmgr_site() returns DB_OPNOTSUP. */
		COMPQUIET(i, 0);
		COMPQUIET(t_ret, 0);
		DB_ASSERT(env, 0);
#endif
		return (ret);
	}

	/* set_cachesize <unsigned gbytes> <unsigned bytes> <int ncaches> */
	if (strcasecmp(argv[0], "set_cachesize") == 0) {
		if (nf != 4)
			goto format;
		CFG_GET_UINT32(argv[1], &uv1);
		CFG_GET_UINT32(argv[2], &uv2);
		CFG_GET_INT(argv[3], &lv1);
		return (__memp_set_cachesize(
		    dbenv, (u_int32_t)uv1, (u_int32_t)uv2, (int)lv1));
	}

	/* set_intermediate_dir <integer dir permission> */
	if (strcasecmp(argv[0], "set_intermediate_dir") == 0) {
		if (nf != 2)
			goto format;
		CFG_GET_INT(argv[1], &lv1);
		if (lv1 <= 0)
			goto format;
		env->dir_mode = (int)lv1;
		return (0);
	}

	/* set_flags <env or log flag name> [on | off] */
	if (strcasecmp(argv[0], "set_flags") == 0) {
		if (nf != 2 && nf != 3)
			goto format;
		onoff = 1;
		if (nf == 3) {
			if (strcasecmp(argv[2], "off") == 0)
				onoff = 0;
			else if (strcasecmp(argv[2], "on") != 0)
				goto format;
		}
		/* First see whether it is an env flag, then a log flag. */
		if ((lv1 = __db_name_to_val(config_set_flags, argv[1])) != -1)
			return (__env_set_flags(dbenv, (u_int32_t)lv1, onoff));
		else if ((lv1 =
		    __db_name_to_val(config_set_flags_forlog, argv[1])) != -1)
			return (__log_set_config(dbenv, (u_int32_t)lv1, onoff));
		goto format;
	}

	/* log_set_config <log flag name> [on | off] */
	if (strcasecmp(argv[0], "log_set_config") == 0) {
		if (nf != 2 && nf != 3)
			goto format;
		onoff = 1;
		if (nf == 3) {
			if (strcasecmp(argv[2], "off") == 0)
				onoff = 0;
			else if (strcasecmp(argv[2], "on") != 0)
				goto format;
		}
		if ((lv1 =
		    __db_name_to_val(config_log_set_config, argv[1])) == -1)
			goto format;
		return (__log_set_config(dbenv, (u_int32_t)lv1, onoff));
	}

	/* set_lk_detect db_lock_xxx */
	if (strcasecmp(argv[0], "set_lk_detect") == 0) {
		if (nf != 2)
			goto format;
		if ((lv1 =
		    __db_name_to_val(config_set_lk_detect, argv[1])) == -1)
			goto format;
		return (__lock_set_lk_detect(dbenv, (u_int32_t)lv1));
	}

	/* set_lock_timeout <unsigned lock timeout> */
	if (strcasecmp(argv[0], "set_lock_timeout") == 0) {
		if (nf != 2)
			goto format;
		CFG_GET_UINT32(argv[1], &uv1);
		return (__lock_set_env_timeout(
		    dbenv, (u_int32_t)uv1, DB_SET_LOCK_TIMEOUT));
	}

	/* set_open_flags <env open flag name> [on | off] */
	if (strcasecmp(argv[0], "set_open_flags") == 0) {
		if (nf != 2 && nf != 3)
			goto format;
		onoff = 1;
		if (nf == 3) {
			if (strcasecmp(argv[2], "off") == 0)
				onoff = 0;
			else if (strcasecmp(argv[2], "on") != 0)
				goto format;
		}
		if ((lv1 =
		    __db_name_to_val(config_set_open_flags, argv[1])) == -1)
			goto format;
		if (onoff == 1)
			FLD_SET(env->open_flags, (u_int32_t)lv1);
		else
			FLD_CLR(env->open_flags, (u_int32_t)lv1);
		return (0);
	}

	/* set_region_init <0 or 1> */
	if (strcasecmp(argv[0], "set_region_init") == 0) {
		if (nf != 2)
			goto format;
		CFG_GET_INT(argv[1], &lv1);
		if (lv1 != 0 && lv1 != 1)
			goto format;
		return (__env_set_flags(
		    dbenv, DB_REGION_INIT, lv1 == 0 ? 0 : 1));
	}

	/* set_reg_timeout <unsigned timeout> */
	if (strcasecmp(argv[0], "set_reg_timeout") == 0) {
		if (nf != 2)
			goto format;
		CFG_GET_UINT32(argv[1], &uv1);
		return (__env_set_timeout(
		    dbenv, (u_int32_t)uv1, DB_SET_REG_TIMEOUT));
	}

	/* set_txn_timeout <unsigned timeout> */
	if (strcasecmp(argv[0], "set_txn_timeout") == 0) {
		if (nf != 2)
			goto format;
		CFG_GET_UINT32(argv[1], &uv1);
		return (__lock_set_env_timeout(
		    dbenv, (u_int32_t)uv1, DB_SET_TXN_TIMEOUT));
	}

	/* set_verbose db_verb_XXX [on | off] */
	if (strcasecmp(argv[0], "set_verbose") == 0) {
		if (nf != 2 && nf != 3)
			goto format;
		onoff = 1;
		if (nf == 3) {
			if (strcasecmp(argv[2], "off") == 0)
				onoff = 0;
			else if (strcasecmp(argv[2], "on") != 0)
				goto format;
		}
		if ((lv1 = __db_name_to_val(config_set_verbose, argv[1])) == -1)
			goto format;
		return (__env_set_verbose(dbenv, (u_int32_t)lv1, onoff));
	}

	__db_errx(env,
	    DB_STR_A("1585", "unrecognized name-value pair: %s", "%s"), s);
	return (EINVAL);
}

/* cmp_cfg_name --
 *	Bsearch comparison function for CFG_DESC.name, for looking up
 *	the names of simple commmands.
 */
static int
cmp_cfg_name(sought, element)
	const void *sought;
	const void *element;
{
	return
	    (strcmp((const char *) sought, ((const CFG_DESC *) element)->name));
}

/*
 * __config_scan --
 *      Split DB_CONFIG lines into fields. Usually each whitespace separated
 *	field is scanned as a distinct argument. However, if the command is
 *	recognized as one needing a single string value, then the rest of the
 *	line is returned as the one argument. That supports strings which
 *	contain whitespaces, such as some directory paths.
 *
 *	This returns the number of fields. It sets *descptr to the command
 *	descriptor (if it is recognized), or NULL.
 */
static int
__config_scan(input, argv, descptr)
	char *input, *argv[CFG_SLOTS];
	const CFG_DESC **descptr;
{
	size_t tablecount;
	int count;
	char **ap;

	tablecount = sizeof(config_descs) / sizeof(config_descs[0]);
	*descptr = NULL;
	for (count = 0, ap = argv; (*ap = strsep(&input, " \t\n")) != NULL;) {
		/* Empty tokens are adjacent whitespaces; skip them. */
		if (**ap == '\0')
			continue;
		/* Accept a non-empty token as the next field. */
		count++;
		ap++;
		/*
		 * If that was the first token, look it up in the simple command
		 * table. If it is there and takes a single string value, then
		 * return the remainder of the line (after skipping over any
		 * leading whitespaces) without splitting it further.
		 */
		if (count == 1) {
			*descptr = bsearch(argv[0], config_descs,
			    tablecount, sizeof(config_descs[0]), cmp_cfg_name);
			if (*descptr != NULL &&
			    (*descptr)->type == CFG_STRING) {
				count++;
				while (isspace(*input))
					input++;
				*ap++ = input;
				break;
			}
		}
		/* Stop scanning if the line has too many tokens. */
		if (count >= CFG_SLOTS)
			break;
	}
	return (count);
}
