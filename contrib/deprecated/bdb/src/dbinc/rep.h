/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef _DB_REP_H_
#define	_DB_REP_H_

#include <contrib/deprecated/bdb/src/dbinc_auto/rep_automsg.h>

#if defined(__cplusplus)
extern "C" {
#endif

/*
 * Names of client temp databases.
 */
#define	REPFILEPREFIX	"__db.rep"
#define	REPDBNAME	"__db.rep.db"
#define	REPPAGENAME     "__db.reppg.db"

/*
 * Name of replicated system database file, and LSN history subdatabase within
 * it.  If the INMEM config flag is set, we create the database in memory, with
 * the REPLSNHIST name (so that is why it also follows the __db naming
 * convention).
 */
#define	REPSYSDBNAME	"__db.rep.system"
#define	REPLSNHIST	"__db.lsn.history"
#define	REPMEMBERSHIP	"__db.membership"
#define	REPSYSDBPGSZ	1024
#define IS_REP_FILE(name)	(strcmp(name, REPSYSDBNAME) == 0)
				    

/* Current version of commit token format, and LSN history database format. */
#define	REP_COMMIT_TOKEN_FMT_VERSION	1
#define	REP_LSN_HISTORY_FMT_VERSION	1

/*
 * Message types
 */
#define	REP_INVALID	0	/* Invalid message type. */
#define	REP_ALIVE	1	/* I am alive message. */
#define	REP_ALIVE_REQ	2	/* Request for alive messages. */
#define	REP_ALL_REQ	3	/* Request all log records greater than LSN. */
#define	REP_BULK_LOG	4	/* Bulk transfer of log records. */
#define	REP_BULK_PAGE	5	/* Bulk transfer of pages. */
#define	REP_DUPMASTER	6	/* Duplicate master detected; propagate. */
#define	REP_FILE	7	/* Page of a database file. NOTUSED */
#define	REP_FILE_FAIL	8	/* File requested does not exist. */
#define	REP_FILE_REQ	9	/* Request for a database file. NOTUSED */
#define	REP_LEASE_GRANT	10	/* Client grants a lease to a master. */
#define	REP_LOG		11	/* Log record. */
#define	REP_LOG_MORE	12	/* There are more log records to request. */
#define	REP_LOG_REQ	13	/* Request for a log record. */
#define	REP_MASTER_REQ	14	/* Who is the master */
#define	REP_NEWCLIENT	15	/* Announces the presence of a new client. */
#define	REP_NEWFILE	16	/* Announce a log file change. */
#define	REP_NEWMASTER	17	/* Announces who the master is. */
#define	REP_NEWSITE	18	/* Announces that a site has heard from a new
				 * site; like NEWCLIENT, but indirect.  A
				 * NEWCLIENT message comes directly from the new
				 * client while a NEWSITE comes indirectly from
				 * someone who heard about a NEWSITE.
				 */
#define	REP_PAGE	19	/* Database page. */
#define	REP_PAGE_FAIL	20	/* Requested page does not exist. */
#define	REP_PAGE_MORE	21	/* There are more pages to request. */
#define	REP_PAGE_REQ	22	/* Request for a database page. */
#define	REP_REREQUEST	23	/* Force rerequest. */
#define	REP_START_SYNC	24	/* Tell client to begin syncing a ckp.*/
#define	REP_UPDATE	25	/* Environment hotcopy information. */
#define	REP_UPDATE_REQ	26	/* Request for hotcopy information. */
#define	REP_VERIFY	27	/* A log record for verification. */
#define	REP_VERIFY_FAIL	28	/* The client is outdated. */
#define	REP_VERIFY_REQ	29	/* Request for a log record to verify. */
#define	REP_VOTE1	30	/* Send out your information for an election. */
#define	REP_VOTE2	31	/* Send a "you are master" vote. */
/*
 * Maximum message number for conversion tables.  Update this
 * value as the largest message number above increases.
 * It might make processing messages more straightforward if
 * the *_MORE and BULK* messages were flags within the regular
 * message type instead of separate message types themselves.
 *
 * !!!
 * NOTE: When changing messages above, the two tables for upgrade support
 * need adjusting.  They are in rep_util.c.
 */
#define	REP_MAX_MSG	31

/*
 * This is the list of client-to-client requests messages.
 * We use this to decide if we're doing client-to-client and
 * might need to send a rerequest.
 */
#define	REP_MSG_REQ(rectype)			\
    (rectype == REP_ALL_REQ ||			\
    rectype == REP_LOG_REQ ||			\
    rectype == REP_PAGE_REQ ||			\
    rectype == REP_VERIFY_REQ)

/*
 * Note that the version information should be at the beginning of the
 * structure, so that we can rearrange the rest of it while letting the
 * version checks continue to work.  DB_REPVERSION should be revved any time
 * the rest of the structure changes or when the message numbers change.
 *
 * Define also, the corresponding log versions that are tied to the
 * replication/release versions.  These are only needed in replication
 * and that is why they're defined here. db_printlog takes notice as well.
 */
#define	DB_LOGVERSION_42	8
#define	DB_LOGVERSION_43	10
#define	DB_LOGVERSION_44	11
#define	DB_LOGVERSION_45	12
#define	DB_LOGVERSION_46	13
#define	DB_LOGVERSION_47	14
#define	DB_LOGVERSION_48	15
#define	DB_LOGVERSION_48p2	16
#define	DB_LOGVERSION_50	17
#define	DB_LOGVERSION_51	17
#define	DB_LOGVERSION_52	18
#define	DB_LOGVERSION_53	19
#define	DB_LOGVERSION_MIN	DB_LOGVERSION_44
#define	DB_REPVERSION_INVALID	0
#define	DB_REPVERSION_44	3
#define	DB_REPVERSION_45	3
#define	DB_REPVERSION_46	4
#define	DB_REPVERSION_47	5
#define	DB_REPVERSION_48	5
#define	DB_REPVERSION_50	5
#define	DB_REPVERSION_51	5
#define	DB_REPVERSION_52	6
#define	DB_REPVERSION_53	7
#define	DB_REPVERSION		DB_REPVERSION_53
#define	DB_REPVERSION_MIN	DB_REPVERSION_44

/*
 * RPRINT - Replication diagnostic output
 * VPRINT - Replication verbose output (superset of RPRINT).
 * REP_PRINT_MESSAGE
 *	Macros for verbose replication messages.
 *
 * Everything using RPRINT will go to the system diag file (if it
 * is configured) and also to the user's verbose output if
 * they have that verbose level configured.
 * Messages using VPRINT do not ever go to the system diag file,
 * but will go to the user's verbose output if configured.
 *
 * Use VPRINT for anything that might be printed on a standard,
 * successful transaction.  Use RPRINT for error paths, rep
 * state changes, elections, etc.
 */
#define	REP_DIAGNAME	"__db.rep.diag%02d"
#define	REP_DIAGSIZE	MEGABYTE
#define	RPRINT(env, x) do {						\
	if ((env)->dbenv->verbose != 0)					\
		(void)__rep_print_system x;				\
} while (0)
#define	VPRINT(env, x) do {						\
	if ((env)->dbenv->verbose != 0)					\
		(void)__rep_print x;					\
} while (0)
#define	REP_PRINT_MESSAGE(env, eid, rp, str, fl) do {			\
	if ((env)->dbenv->verbose != 0)					\
		__rep_print_message(env, eid, rp, str, fl);		\
} while (0)

/*
 * Election gen file name
 * The file contains an egen number for an election this client has NOT
 * participated in.  I.e. it is the number of a future election.  We
 * create it when we create the rep region, if it doesn't already exist
 * and initialize egen to 1.  If it does exist, we read it when we create
 * the rep region.  We write it immediately before sending our VOTE1 in
 * an election.  That way, if a client has ever sent a vote for any
 * election, the file is already going to be updated to reflect a future
 * election, should it crash.
 */
#define	REP_EGENNAME	"__db.rep.egen"
#define	REP_GENNAME	"__db.rep.gen"

/*
 * Internal init flag file name:
 * The existence of this file serves as an indication that the client is in the
 * process of Internal Initialization, in case it crashes before completing.
 * During internal init the client's partially reconstructed database pages and
 * logs may be in an inconsistent state, so much so that running recovery must
 * be avoided.  Furthermore, there is no other way to reliably recognize this
 * condition.  Therefore, when we open an environment, and we're just about to
 * run recovery, we check for this file first.  If it exists we must discard all
 * logs and databases.  This avoids the recovery problems, and leads to a fresh
 * attempt at internal init if the environment becomes a replication client and
 * finds a master.  The list of databases which may need to be removed is stored
 * in this file.
 */
#define	REP_INITNAME	"__db.rep.init"
#define	REP_INITVERSION_46	1
#define	REP_INITVERSION_47	2
#define	REP_INITVERSION		3

/*
 * Database types for __rep_client_dbinit
 */
typedef enum {
	REP_DB,		/* Log record database. */
	REP_PG		/* Pg database. */
} repdb_t;

/* Macros to lock/unlock the replication region as a whole. */
#define	REP_SYSTEM_LOCK(env)						\
	MUTEX_LOCK(env, (env)->rep_handle->region->mtx_region)
#define	REP_SYSTEM_UNLOCK(env)						\
	MUTEX_UNLOCK(env, (env)->rep_handle->region->mtx_region)

/*
 * Macros for manipulating the event synchronization.  We use a separate mutex
 * so that an application's call-back function can be invoked without locking
 * the whole region.
 */
#define	REP_EVENT_LOCK(env)						\
	MUTEX_LOCK(env, (env)->rep_handle->region->mtx_event)
#define	REP_EVENT_UNLOCK(env)						\
	MUTEX_UNLOCK(env, (env)->rep_handle->region->mtx_event)

/*
 * Synchronization states
 * Please change __rep_syncstate_to_string (rep_stat.c) to track any changes
 * made to these states.
 *
 * The states are in alphabetical order (except for OFF).  The usual
 * order of progression for a full internal init is:
 * VERIFY, UPDATE, PAGE, LOG (then back to OFF when we're done).
 */
typedef enum {
	SYNC_OFF,	/* No recovery. */
	SYNC_LOG,	/* Recovery - log. */
	SYNC_PAGE,	/* Recovery - pages. */
	SYNC_UPDATE,	/* Recovery - update. */
	SYNC_VERIFY 	/* Recovery - verify. */
} repsync_t;

/*
 * A record of the contents of the VOTE1 msg we sent out at current egen, in
 * case we need to send out a duplicate VOTE1 to a late-joining client in a full
 * election.  The nsites, nvotes, and priority fields of the REP struct can't be
 * used, because those could change.  It's only safe to send out a dup if we
 * send out the exact same info.
 */
typedef struct {
	DB_LSN lsn;
	u_int32_t nsites;
	u_int32_t nvotes;
	u_int32_t priority;
	u_int32_t tiebreaker;
	u_int32_t ctlflags;
	u_int32_t data_gen;
} VOTE1_CONTENT;

/*
 * REP --
 * Shared replication structure.
 */
typedef struct __rep { /* SHARED */
	db_mutex_t	mtx_region;	/* Region mutex. */
	db_mutex_t	mtx_clientdb;	/* Client database mutex. */
	db_mutex_t	mtx_ckp;	/* Checkpoint mutex. */
	db_mutex_t	mtx_diag;	/* Diagnostic message mutex. */
	db_mutex_t	mtx_repstart;	/* Role change mutex. */
	int		diag_index;	/* Diagnostic file index. */
	off_t		diag_off;	/* Diagnostic message offset. */
	roff_t		lease_off;	/* Offset of the lease table. */
	roff_t		tally_off;	/* Offset of the tally region. */
	roff_t		v2tally_off;	/* Offset of the vote2 tally region. */
	int		eid;		/* Environment id. */
	int		master_id;	/* ID of the master site. */
	u_int32_t	version;	/* Current replication version. */
	u_int32_t	egen;		/* Replication election generation. */
	u_int32_t	spent_egen;	/* Egen satisfied by rep_elect call. */
	u_int32_t	gen;		/* Replication generation number. */
	u_int32_t	mgen;		/* Master gen seen by client. */
	u_int32_t	asites;		/* Space allocated for sites. */
	u_int32_t	nsites;		/* Number of sites in group. */
	u_int32_t	nvotes;		/* Number of votes needed. */
	u_int32_t	priority;	/* My priority in an election. */
	u_int32_t	config_nsites;

	db_timeout_t	elect_timeout;	/* Normal/full election timeouts. */
	db_timeout_t	full_elect_timeout;

	db_timeout_t	chkpt_delay;	/* Master checkpoint delay. */

#define	REP_DEFAULT_THROTTLE	(10 * MEGABYTE) /* Default value is < 1Gig. */
	u_int32_t	gbytes;		/* Limit on data sent in single... */
	u_int32_t	bytes;		/* __rep_process_message call. */
#define	DB_REP_REQUEST_GAP	40000	/* 40 msecs */
#define	DB_REP_MAX_GAP		1280000	/* 1.28 seconds */
	db_timespec	request_gap;	/* Minimum time to wait before we
					 * request a missing log record. */
	db_timespec	max_gap;	/* Maximum time to wait before
					 * requesting a missing log record. */
	/* Status change information */
	u_int32_t	apply_th;	/* Number of callers in rep_apply. */
	u_int32_t	arch_th;	/* Number of callers in log_archive. */
	u_int32_t	elect_th;	/* Elect threads in lock-out. */
	u_int32_t	msg_th;		/* Number of callers in rep_proc_msg.*/
	u_int32_t	handle_cnt;	/* Count of handles in library. */
	u_int32_t	op_cnt;		/* Multi-step operation count.*/
	DB_LSN		ckp_lsn;	/* LSN for syncing a checkpoint. */
	DB_LSN		max_prep_lsn;	/* Max LSN of txn_prepare record. */

	/*
	 * Event notification synchronization: the mtx_event and associate
	 * fields which it protects govern event notification to the
	 * application.  They form a guarantee that no matter how crazy the
	 * thread scheduling gets, the application sees a sensible, orderly
	 * progression of events.
	 */
	db_mutex_t	mtx_event;	/* Serializes event notification. */
	/*
	 * Latest generation whose NEWMASTER event the application has been
	 * notified of.  Also serves to force STARTUPDONE to occur after
	 * NEWMASTER.
	 */
	u_int32_t	newmaster_event_gen;
	/*
	 * Latest local victory of an election that the application has been
	 * notified of, expressed as the election generation number.  This
	 * ensures we notify the application exactly once when it wins an
	 * election.
	 */
	u_int32_t	notified_egen;

	/* Internal init information. */
	u_int32_t	nfiles;		/* Number of files we have info on. */
	u_int32_t	curfile;	/* Cur file we're getting (0-based). */
	roff_t		originfo_off;	/* Offset of original file info. */
	u_int32_t	infolen;	/* Remaining length file info buffer. */
	u_int32_t	originfolen;	/* Original length file info buffer. */
	u_int32_t	infoversion;	/* Original file info version. */
	DB_LSN		first_lsn;	/* Earliest LSN we need. */
	u_int32_t	first_vers;	/* Log version of first log file. */
	DB_LSN		last_lsn;	/* Latest LSN we need. */
	/* These are protected by mtx_clientdb. */
	db_timespec	last_pg_ts;	/* Last page stored timestamp. */
	db_pgno_t	ready_pg;	/* Next pg expected. */
	db_pgno_t	waiting_pg;	/* First pg after gap. */
	db_pgno_t	max_wait_pg;	/* Maximum pg requested. */
	u_int32_t	npages;		/* Num of pages rcvd for this file. */
	roff_t		curinfo_off;	/* Offset of current file info. */
					/* Always access with GET_CURINFO(). */

	/* Vote tallying information. */
	u_int32_t	sites;		/* Sites heard from. */
	int		winner;		/* Current winner EID. */
	u_int32_t	w_priority;	/* Winner priority. */
	u_int32_t	w_gen;		/* Winner generation. */
	u_int32_t	w_datagen;	/* Winner data generation. */
	DB_LSN		w_lsn;		/* Winner LSN. */
	u_int32_t	w_tiebreaker;	/* Winner tiebreaking value. */
	u_int32_t	votes;		/* Number of votes for this site. */

	VOTE1_CONTENT	vote1;		/* Valid until rep->egen changes. */

	db_timespec	etime;		/* Election start timestamp. */
	int		full_elect;	/* Is current election a "full" one? */

	/* Leases. */
	db_timeout_t	lease_timeout;	/* Lease timeout. */
	db_timespec	lease_duration;	/* Lease timeout with clock skew. */
	u_int32_t	clock_skew;	/* Clock skew. */
	u_int32_t	clock_base;	/* Clock scale factor base. */
	db_timespec	grant_expire;	/* Local grant expiration time. */

	/* Cached LSN history, matching current gen. */
	DB_LSN		gen_base_lsn;	/* Base LSN of current generation. */
	u_int32_t	master_envid;	/* Current master's "unique" env ID. */

	SH_TAILQ_HEAD(__wait) waiters;	/* List of threads in txn_applied(). */
	SH_TAILQ_HEAD(__wfree) free_waiters;/* Free list of waiter structs. */

#ifdef HAVE_REPLICATION_THREADS
	/*
	 * Replication Framework (repmgr) shared config information.
	 */
	db_mutex_t	mtx_repmgr;	/* Region mutex. */
	roff_t		siteinfo_off;	/* Offset of site array region. */
	u_int		site_cnt;	/* Array slots in use. */
	u_int		site_max;	/* Total array slots allocated. */
	int		self_eid;	/* Where to find the local site. */
	u_int		siteinfo_seq;	/* Number of updates to this info. */
	u_int32_t	min_log_file;	/* Earliest log needed by repgroup. */

	pid_t		listener;

	int		perm_policy;
	db_timeout_t	ack_timeout;
	db_timeout_t	election_retry_wait;
	db_timeout_t	connection_retry_wait;
	db_timeout_t	heartbeat_frequency; /* Max period between msgs. */
	db_timeout_t	heartbeat_monitor_timeout;
#endif  /* HAVE_REPLICATION_THREADS */

	/* Statistics. */
	DB_REP_STAT	stat;
#if defined(HAVE_REPLICATION_THREADS) && defined(HAVE_STATISTICS)
	DB_REPMGR_STAT	mstat;
#endif

	/*
	 * Please change __rep_print_all (rep_stat.c) to track any changes made
	 * to all these flag families below.
	 */
	/* Configuration. */
#define	REP_C_2SITE_STRICT	0x00001		/* Don't cheat on elections. */
#define	REP_C_AUTOINIT		0x00002		/* Auto initialization. */
#define	REP_C_AUTOROLLBACK	0x00004		/* Discard client txns: sync. */
#define	REP_C_BULK		0x00008		/* Bulk transfer. */
#define	REP_C_DELAYCLIENT	0x00010		/* Delay client sync-up. */
#define	REP_C_ELECTIONS		0x00020		/* Repmgr to use elections. */
#define	REP_C_INMEM		0x00040		/* In-memory replication. */
#define	REP_C_LEASE		0x00080		/* Leases configured. */
#define	REP_C_NOWAIT		0x00100		/* Immediate error return. */
	u_int32_t	config;		/* Configuration flags. */

	/* Election. */
#define	REP_E_PHASE0		0x00000001	/* In phase 0 of election. */
#define	REP_E_PHASE1		0x00000002	/* In phase 1 of election. */
#define	REP_E_PHASE2		0x00000004	/* In phase 2 of election. */
#define	REP_E_TALLY		0x00000008	/* Tallied vote before elect. */
	u_int32_t	elect_flags;	/* Election flags. */

	/* Lockout. */
#define	REP_LOCKOUT_API		0x00000001	/* BDB API - handle_cnt. */
#define	REP_LOCKOUT_APPLY	0x00000002	/* apply msgs - apply_th. */
#define	REP_LOCKOUT_ARCHIVE	0x00000004	/* log_archive. */
#define	REP_LOCKOUT_MSG		0x00000008	/* Message process - msg_th. */
#define	REP_LOCKOUT_OP		0x00000010	/* BDB ops txn,curs - op_cnt. */
	u_int32_t	lockout_flags;	/* Lockout flags. */

	/* See above for enumerated sync states. */
	repsync_t	sync_state;	/* Recovery/synchronization flags. */

	/*
	 * When adding a new flag value, consider whether it should be
	 * cleared in rep_start() when starting as a master or a client.
	 */
#define	REP_F_ABBREVIATED	0x00000001	/* Recover NIMDB pages only. */
#define	REP_F_APP_BASEAPI	0x00000002	/* Base API application. */
#define	REP_F_APP_REPMGR	0x00000004	/* repmgr application. */
#define	REP_F_CLIENT		0x00000008	/* Client replica. */
#define	REP_F_DELAY		0x00000010	/* Delaying client sync-up. */
#define	REP_F_GROUP_ESTD	0x00000020	/* Rep group is established. */
#define	REP_F_INUPDREQ		0x00000040	/* Thread in rep_update_req. */
#define	REP_F_LEASE_EXPIRED	0x00000080	/* Leases guaranteed expired. */
#define	REP_F_MASTER		0x00000100	/* Master replica. */
#define	REP_F_MASTERELECT	0x00000200	/* Master elect. */
#define	REP_F_NEWFILE		0x00000400	/* Newfile in progress. */
#define	REP_F_NIMDBS_LOADED	0x00000800	/* NIMDBs are materialized. */
#define	REP_F_SKIPPED_APPLY	0x00001000	/* Skipped applying a record. */
#define	REP_F_START_CALLED	0x00002000	/* Rep_start called. */
#define	REP_F_SYS_DB_OP		0x00004000	/* Operation in progress. */
	u_int32_t	flags;
} REP;

/* Information about a thread waiting in txn_applied(). */
typedef enum {
	AWAIT_GEN,		/* Client's gen is behind token gen. */
	AWAIT_HISTORY,		/* Haven't received master's LSN db update. */
	AWAIT_LSN,		/* Awaiting replication of user txn. */
	AWAIT_NIMDB,		/* LSN db missing: maybe it's INMEM. */
	LOCKOUT			/* Thread awoken due to pending lockout. */
} rep_waitreason_t;

struct rep_waitgoal {
	rep_waitreason_t	why;
	union {
		DB_LSN	lsn;	/* For AWAIT_LSN and AWAIT_HISTORY. */
		u_int32_t gen;	/* AWAIT_GEN */
	} u;
};

struct __rep_waiter {
	db_mutex_t	mtx_repwait; /* Self-blocking mutex. */
	struct rep_waitgoal	goal;
	SH_TAILQ_ENTRY	links;	     /* On either free or waiting list. */

#define	REP_F_PENDING_LOCKOUT	0x00000001
#define	REP_F_WOKEN		0x00000002
	u_int32_t	flags;
};

/*
 * Macros to check and clear the BDB lockouts.  Currently they are
 * locked out/set individually because they pertain to different pieces of
 * the BDB API, they are otherwise always checked and cleared together.
 */
#define ISSET_LOCKOUT_BDB(R) 						\
    (FLD_ISSET((R)->lockout_flags, (REP_LOCKOUT_API | REP_LOCKOUT_OP)))

#define CLR_LOCKOUT_BDB(R) 						\
    (FLD_CLR((R)->lockout_flags, (REP_LOCKOUT_API | REP_LOCKOUT_OP)))

/*
 * Recovery flag mask to easily check any/all recovery bits.  That is
 * REP_LOCKOUT_{API|OP} and most REP_S_*.  This must change if the values
 * of the flags change.  NOTE:  We do not include REP_LOCKOUT_MSG in
 * this mask because it is used frequently in non-recovery related
 * areas and we want to manipulate it separately (see especially
 * in __rep_new_master).
 */
#define CLR_RECOVERY_SETTINGS(R)					\
do {									\
	(R)->sync_state = SYNC_OFF;					\
	CLR_LOCKOUT_BDB(R);						\
} while (0)

#define	IS_REP_RECOVERING(R)						\
    ((R)->sync_state != SYNC_OFF || ISSET_LOCKOUT_BDB(R))

/*
 * REP_F_EPHASE0 is not a *real* election phase.  It is used for
 * master leases and allowing the client to find the master or
 * expire its lease.  However, EPHASE0 is cleared by __rep_elect_done.
 */
#define	IN_ELECTION(R)							\
	FLD_ISSET((R)->elect_flags, REP_E_PHASE1 | REP_E_PHASE2)
#define	IN_ELECTION_TALLY(R) \
	FLD_ISSET((R)->elect_flags, REP_E_PHASE1 | REP_E_PHASE2 | REP_E_TALLY)
#define	ELECTION_MAJORITY(n) (((n) / 2) + 1)

#define	IN_INTERNAL_INIT(R) \
	((R)->sync_state == SYNC_LOG || (R)->sync_state == SYNC_PAGE)

#define	IS_REP_MASTER(env)						\
	(REP_ON(env) &&							\
	    F_ISSET(((env)->rep_handle->region), REP_F_MASTER))

#define	IS_REP_CLIENT(env)						\
	(REP_ON(env) &&							\
	    F_ISSET(((env)->rep_handle->region), REP_F_CLIENT))

#define	IS_REP_STARTED(env)						\
	(REP_ON(env) &&							\
	    F_ISSET(((env)->rep_handle->region), REP_F_START_CALLED))

#define	IS_USING_LEASES(env)						\
	(REP_ON(env) &&							\
	    FLD_ISSET(((env)->rep_handle->region)->config, REP_C_LEASE))

#define	IS_CLIENT_PGRECOVER(env)					\
	(IS_REP_CLIENT(env) &&						\
	    (((env)->rep_handle->region)->sync_state ==  SYNC_PAGE))

/*
 * Macros to figure out if we need to do replication pre/post-amble processing.
 * Skip for specific DB handles owned by the replication layer, either because
 * replication is running recovery or because it's a handle entirely owned by
 * the replication code (replication opens its own databases to track state).
 */
#define REP_FLAGS_SET(env)						\
	((env)->rep_handle->region->flags != 0 ||			\
	(env)->rep_handle->region->elect_flags != 0 ||			\
	(env)->rep_handle->region->lockout_flags != 0)

#define	IS_ENV_REPLICATED(env)						\
	(REP_ON(env) && REP_FLAGS_SET(env))

/*
 * Update the temporary log archive block timer.
 */
#define	MASTER_UPDATE(env, renv) do {					\
	REP_SYSTEM_LOCK(env);						\
	F_SET((renv), DB_REGENV_REPLOCKED);				\
	(void)time(&(renv)->op_timestamp);				\
	REP_SYSTEM_UNLOCK(env);						\
} while (0)

/*
 * Macro to set a new generation number.  Cached values from the LSN history
 * database are associated with the current gen, so when the gen changes we must
 * invalidate the cache.  Use this macro for all gen changes, to avoid
 * forgetting to do so.  This macro should be used while holding the rep system
 * mutex (unless we know we're single-threaded for some other reason, like at
 * region create time).
 */
#define	SET_GEN(g) do {							\
	rep->gen = (g);							\
	ZERO_LSN(rep->gen_base_lsn);					\
} while (0)


/*
 * Gap processing flags.  These provide control over the basic
 * gap processing algorithm for some special cases.
 */
#define	REP_GAP_FORCE		0x001	/* Force a request for a gap. */
#define	REP_GAP_REREQUEST	0x002	/* Gap request is a forced rerequest. */
					/* REREQUEST is a superset of FORCE. */

/*
 * Flags indicating what kind of record we want to back up to, in the log.
 */
#define	REP_REC_COMMIT		0x001 	/* Most recent commit record. */
#define	REP_REC_PERM		0x002	/* Most recent perm record. */
					/* PERM is a superset of COMMIT. */

/*
 * Basic pre/post-amble processing.
 */
#define	REPLICATION_WRAP(env, func_call, checklock, ret) do {		\
	int __rep_check, __t_ret;					\
	__rep_check = IS_ENV_REPLICATED(env) ? 1 : 0;			\
	(ret) = __rep_check ? __env_rep_enter(env, checklock) : 0;	\
	if ((ret) == 0) {						\
		(ret) = func_call;					\
		if (__rep_check && (__t_ret =				\
		    __env_db_rep_exit(env)) != 0 && (ret) == 0)		\
		(ret) = __t_ret;					\
	}								\
} while (0)

/*
 * Macro to safely access curinfo and its internal DBT pointers from
 * any process.  This should always be used to access curinfo.  If
 * the internal DBT pointers are to be used, mtx_clientdb must be held
 * between the time of this call and the use of the pointers.
 *
 * The current file information (curinfo) is stored in shared region
 * memory and accessed via an offset.  It contains DBTs that themselves
 * point to allocated data.  __rep_nextfile() manages this information in a
 * single chunk of shared memory.
 *
 * If different processes access curinfo, they may have different shared
 * region addresses.  This means that curinfo and its pointers to DBT data
 * must be recalculated for each process starting with the offset.
 */
#define GET_CURINFO(rep, infop, curinfo)				\
do {									\
	curinfo = R_ADDR(infop, rep->curinfo_off);			\
	if ((curinfo)->uid.size > 0)					\
		(curinfo)->uid.data = R_ADDR(infop,			\
		    rep->curinfo_off + sizeof(__rep_fileinfo_args));	\
	else								\
		(curinfo)->uid.data = NULL;				\
	if ((curinfo)->info.size > 0)					\
		(curinfo)->info.data = R_ADDR(infop, rep->curinfo_off +	\
		    sizeof(__rep_fileinfo_args) + (curinfo)->uid.size);	\
	else								\
		(curinfo)->info.data = NULL;				\
	if ((curinfo)->dir.size > 0)					\
		(curinfo)->dir.data = R_ADDR(infop, rep->curinfo_off +	\
		    sizeof(__rep_fileinfo_args) + (curinfo)->uid.size +	\
		    (curinfo)->info.size);				\
	else								\
		(curinfo)->dir.data = NULL;				\
} while (0)

/*
 * Per-process replication structure.
 *
 * There are 2 mutexes used in the Base replication API.  (See LOCK_MUTEX in
 * repmgr.h for a discussion of repmgr.)
 * 1.  mtx_region - This protects the fields of the rep region above.
 * 2.  mtx_clientdb - This protects the per-process flags, and bookkeeping
 * database and all of the components that maintain it.  Those
 * components include the following fields in the log region (see log.h):
 *	a. ready_lsn
 *	b. waiting_lsn
 *	c. verify_lsn
 *	d. wait_recs
 *	e. rcvd_recs
 *	f. max_wait_lsn
 * These fields in the log region are NOT protected by the log region lock at
 * all.
 *
 * Note that the per-process flags should truly be protected by a special
 * per-process thread mutex, but it is currently set in so isolated a manner
 * that it didn't make sense to do so and in most case we're already holding
 * the mtx_clientdb anyway.
 *
 * The lock ordering protocol is that mtx_clientdb must be acquired first and
 * then either REP->mtx_region, or the LOG->mtx_region mutex may be acquired if
 * necessary.
 *
 * Note that the appropriate mutex is needed any time one or more related
 * values are read or written that could possibly use more than one atomic
 * machine instruction.  A single 32-bit integer value is safe without a
 * mutex, but most other types of value should use a mutex.
 *
 * Any use of a mutex must be inside a matched pair of ENV_ENTER() and
 * ENV_LEAVE() macros.  This ensures that if a thread dies while holding
 * a lock (i.e. a mutex), recovery can clean it up so that it does not
 * indefinitely block other threads.
 */
struct __db_rep {
	/*
	 * Shared configuration information -- copied to and maintained in the
	 * shared region as soon as the shared region is created.
	 */
	int		eid;		/* Environment ID. */

	u_int32_t	gbytes;		/* Limit on data sent in single... */
	u_int32_t	bytes;		/* __rep_process_message call. */

	db_timespec	request_gap;	/* Minimum time to wait before we
					 * request a missing log record. */
	db_timespec	max_gap;	/* Maximum time to wait before
					 * requesting a missing log record. */

	u_int32_t	clock_skew;	/* Clock skew factor. */
	u_int32_t	clock_base;	/* Clock skew base. */
	u_int32_t	config;		/* Configuration flags. */
	u_int32_t	config_nsites;

	db_timeout_t	elect_timeout;	/* Normal/full election timeouts. */
	db_timeout_t	full_elect_timeout;

	db_timeout_t	chkpt_delay;	/* Master checkpoint delay. */

	u_int32_t	my_priority;
	db_timeout_t	lease_timeout;	/* Master leases. */
	/*
	 * End of shared configuration information.
	 */
	int		(*send)		/* Send function. */
			    __P((DB_ENV *, const DBT *, const DBT *,
			    const DB_LSN *, int, u_int32_t));

	DB		*rep_db;	/* Bookkeeping database. */
	DB		*lsn_db;	/* (Replicated) LSN history database. */

	REP		*region;	/* In memory structure. */
	u_int8_t	*bulk;		/* Shared memory bulk area. */

#define	DBREP_DIAG_FILES	2
	DB_FH		*diagfile[DBREP_DIAG_FILES];	/* Diag files fhp. */
	off_t		diag_off;	/* Current diag file offset. */

	/* These are protected by mtx_clientdb. */
	DB_MPOOLFILE	*file_mpf;	/* Mpoolfile for current database. */
	DB		*file_dbp;	/* This file's page info. */
	DBC		*queue_dbc;	/* Dbc for a queue file. */

	/*
	 * Please change __rep_print_all (rep_stat.c) to track any changes made
	 * to these flags.
	 */
#define	DBREP_APP_BASEAPI	0x0001	/* Base API application. */
#define	DBREP_APP_REPMGR	0x0002	/* repmgr application. */
#define	DBREP_OPENFILES		0x0004	/* This handle has opened files. */
	u_int32_t	flags;		/* per-process flags. */

#ifdef HAVE_REPLICATION_THREADS
	/*
	 * Replication Framework (repmgr) per-process information.
	 */
	u_int		nthreads;	/* Msg processing threads. */
	u_int		athreads;	/* Space allocated for msg threads. */
	u_int		non_rep_th;	/* Threads in GMDB or channel msgs. */
	u_int		aelect_threads; /* Space allocated for elect threads. */
	u_int32_t	init_policy;
	int		perm_policy;
	DB_LSN		perm_lsn; /* Last perm LSN we've announced. */
	db_timeout_t	ack_timeout;
	db_timeout_t	election_retry_wait;
	db_timeout_t	connection_retry_wait;
	db_timeout_t	heartbeat_frequency; /* Max period between msgs. */
	db_timeout_t	heartbeat_monitor_timeout;

	/* Thread synchronization. */
	REPMGR_RUNNABLE *selector, **messengers, **elect_threads;
	REPMGR_RUNNABLE	*preferred_elect_thr;
	db_timespec	repstart_time;
	mgr_mutex_t	*mutex;
	cond_var_t	check_election, gmdb_idle, msg_avail;
	waiter_t	ack_waiters; /* For threads awaiting PERM acks. */
#ifdef DB_WIN32
	HANDLE		signaler;
#else
	int		read_pipe, write_pipe;
#endif

	/* Operational stuff. */
	REPMGR_SITE	*sites;		/* Array of known sites. */
	u_int		site_cnt;	/* Array slots in use. */
	u_int		site_max;	/* Total array slots allocated. */
	int		self_eid;	/* Where to find the local site. */
	u_int		siteinfo_seq;	/* Last known update to this list. */

	/*
	 * The connections list contains only those connections not actively
	 * associated with a known site (see repmgr.h).
	 */
	CONNECTION_LIST	connections;
	RETRY_Q_HEADER	retries;	/* Sites needing connection retry. */
	struct {
		int	size;
		STAILQ_HEAD(__repmgr_q_header, __repmgr_message) header;
	} input_queue;

	socket_t	listen_fd;
	db_timespec	last_bcast;	/* Time of last broadcast msg. */

	/*
	 * Status of repmgr.  It is ready when repmgr is not yet started.  It
	 * is running after repmgr is (re)started.  It is stopped if the env
	 * of the running repmgr is closed, or the site is removed. 
	 */
	enum { ready, running, stopped } repmgr_status;
	int		new_connection;	  /* Since last master seek attempt. */
	int		takeover_pending; /* We've been elected master. */
	int		gmdb_busy;
	int		client_intent;	/* Will relinquish master role. */
	int		gmdb_dirty;
	int		have_gmdb;
	int		seen_repmsg;

	/*
	 * Flag to show what kind of transaction is currently in progress.
	 * Primary means we're doing the first (critical) phase of a membership
	 * DB update, where we care about perm failures.  In the secondary phase
	 * we don't care.  Usually the value is "none", when normal user
	 * transactions are happening.  We need to use this global flag because
	 * we don't have a more proper direct channel to communicate information
	 * between the originator of a transaction and the replication send()
	 * function that has to wait for acks and decide what to do about them.
	 */ 
	enum { none, gmdb_primary, gmdb_secondary } active_gmdb_update;
	int		limbo_resolution_needed;

	/*
	 * GMDB update sequence count.  On creation we write version 1; so, once
	 * repmgr has started and tried to read, a 0 here can be taken to mean
	 * that the DB doesn't exist yet.
	 */
	u_int32_t	membership_version;
	u_int32_t	member_version_gen;

	/* LSN of GMDB txn that got a perm failure. */
	DB_LSN		limbo_failure;
	/* EID whose membership status is therefore unresolved */
	int		limbo_victim;
	/* LSN of a later txn that achieves perm success. */
	DB_LSN		durable_lsn;
	DB		*gmdb;	/* Membership database handle. */
	/*
	 * Membership list restored from init file after crash during internal init.
	 */
	u_int8_t	*restored_list;
	size_t		restored_list_length;

	/* Application's message dispatch call-back function. */
	void  (*msg_dispatch) __P((DB_ENV *, DB_CHANNEL *,
		DBT *, u_int32_t, u_int32_t));
#endif  /* HAVE_REPLICATION_THREADS */
};

/*
 * Determine whether application is repmgr or base replication API.  If
 * repmgr was configured, base the test on internal replication flags for
 * APP_REPMGR and APP_BASEAPI.  These flags get set by the appropriate parts
 * of the various replication APIs.
 */
#ifdef HAVE_REPLICATION_THREADS
/*
 * Application type is set to be repmgr when:
 *   1. A local site is defined.
 *   2. A remote site is defined.
 *   3. An acknowledgement policy is configured.
 *   4. A repmgr flag is configured.
 *   5. A timeout value is configured for one of the repmgr timeouts.
 */
#define	APP_IS_REPMGR(env)						\
	(REP_ON(env) ?							\
	    F_ISSET((env)->rep_handle->region, REP_F_APP_REPMGR) :	\
	    F_ISSET((env)->rep_handle, DBREP_APP_REPMGR))

/*
 * Application type is set to be base replication API when:
 *   1. Transport send function is defined and is not the repmgr send
 *      function.
 */
#define	APP_IS_BASEAPI(env)						\
	(REP_ON(env) ?							\
	    F_ISSET((env)->rep_handle->region, REP_F_APP_BASEAPI) :	\
	    F_ISSET((env)->rep_handle, DBREP_APP_BASEAPI))

/*
 * Set application type.  These macros do extra checking to guarantee that
 * only one application type is ever set.
 */
#define	APP_SET_REPMGR(env) do {					\
	if (REP_ON(env)) {						\
		ENV_ENTER(env, ip);					\
		REP_SYSTEM_LOCK(env);					\
		if (!F_ISSET((env)->rep_handle->region,			\
		    REP_F_APP_BASEAPI))					\
			F_SET((env)->rep_handle->region,		\
			    REP_F_APP_REPMGR);				\
		REP_SYSTEM_UNLOCK(env);					\
		ENV_LEAVE(env, ip);					\
	} else if (!F_ISSET((env)->rep_handle, DBREP_APP_BASEAPI))	\
		F_SET((env)->rep_handle, DBREP_APP_REPMGR);		\
} while (0)
#define	APP_SET_BASEAPI(env) do {					\
	if (REP_ON(env)) {						\
		ENV_ENTER(env, ip);					\
		REP_SYSTEM_LOCK(env);					\
		if (!F_ISSET((env)->rep_handle->region,			\
		    REP_F_APP_REPMGR))					\
			F_SET((env)->rep_handle->region,		\
			    REP_F_APP_BASEAPI);				\
		REP_SYSTEM_UNLOCK(env);					\
		ENV_LEAVE(env, ip);					\
	} else if (!F_ISSET((env)->rep_handle, DBREP_APP_REPMGR))	\
		F_SET((env)->rep_handle, DBREP_APP_BASEAPI);		\
} while (0)

#else
/*
 * We did not configure repmgr, application must be base replication API.
 * The APP_SET_* macros are noops in this case, but they must be defined
 * with a null body to avoid compiler warnings on some platforms.
 */
#define	APP_IS_REPMGR(env) 0
#define	APP_SET_REPMGR(env) do {					\
	;								\
} while (0)
#define	APP_IS_BASEAPI(env) 1
#define	APP_SET_BASEAPI(env) do {					\
	;								\
} while (0)
#endif  /* HAVE_REPLICATION_THREADS */

/*
 * Control structure flags for replication communication infrastructure.
 */
/*
 * Define old DB_LOG_ values that we must support here.  For reasons of
 * compatibility with old versions, these values must be reserved explicitly in
 * the list of flag values (below)
 */
#define	DB_LOG_PERM_42_44	0x20
#define	DB_LOG_RESEND_42_44	0x40
#define	REPCTL_INIT_45		0x02	/* Back compatible flag value. */

#define	REPCTL_ELECTABLE	0x01	/* Upgraded client is electable. */
#define	REPCTL_FLUSH		0x02	/* Record should be flushed. */
#define	REPCTL_GROUP_ESTD	0x04	/* Message from site in a group. */
#define	REPCTL_INIT		0x08	/* Internal init message. */
#define	REPCTL_LEASE		0x10	/* Lease related message.. */
			/*
			 * Skip over reserved values 0x20
			 * and 0x40, as explained above.
			 */
#define	REPCTL_LOG_END		0x80	/* Approximate end of group-wide log. */
#define	REPCTL_PERM		DB_LOG_PERM_42_44
#define	REPCTL_RESEND		DB_LOG_RESEND_42_44

/*
 * File info flags for internal init.  The per-database (i.e., file) flag
 * represents the on-disk format of the file, and is conveyed from the master to
 * the initializing client in the UPDATE message, so that the client can know
 * how to create the file.  The per-page flag is conveyed along with each PAGE
 * message, describing the format of the page image being transmitted; it is of
 * course set by the site serving the PAGE_REQ.  The serving site gets the page
 * image from its own mpool, and thus the page is in the native format of the
 * serving site.  This format may be different (i.e., opposite) from the on-disk
 * format, and in fact can vary per-page, since with client-to-client sync it is
 * possible for various different sites to serve the various PAGE_REQ requests.
 */
#define	REPINFO_DB_LITTLEENDIAN	0x0001	/* File is little-endian lorder. */
#define	REPINFO_PG_LITTLEENDIAN	0x0002	/* Page is little-endian lorder. */

/*
 * Control message format for 4.6 release.  The db_timespec_t is
 * not a portable structure.  Therefore, in 4.6, replication among
 * mixed OSs such as Linux and Windows, which have different time_t
 * sizes, does not work.
 */
typedef struct {
	u_int32_t	rep_version;	/* Replication version number. */
	u_int32_t	log_version;	/* Log version number. */

	DB_LSN		lsn;		/* Log sequence number. */
	u_int32_t	rectype;	/* Message type. */
	u_int32_t	gen;		/* Generation number. */
	db_timespec	msg_time;	/* Timestamp seconds for leases. */
	u_int32_t	flags;		/* log_put flag value. */
} REP_46_CONTROL;

/*
 * Control message format for 4.5 release and earlier.
 */
typedef struct {
	u_int32_t	rep_version;	/* Replication version number. */
	u_int32_t	log_version;	/* Log version number. */

	DB_LSN		lsn;		/* Log sequence number. */
	u_int32_t	rectype;	/* Message type. */
	u_int32_t	gen;		/* Generation number. */
	u_int32_t	flags;		/* log_put flag value. */
} REP_OLD_CONTROL;

#define	LEASE_REFRESH_MIN	30	/* Minimum number of refresh retries. */
#define	LEASE_REFRESH_USEC	50000	/* Microseconds between refresh tries. */

/* Master granted lease information. */
typedef struct __rep_lease_entry {
	int		eid;		/* EID of client grantor. */
	db_timespec	start_time;	/* Start time clients echo back. */
	db_timespec	end_time;	/* Master lease expiration time. */
	DB_LSN		lease_lsn;	/* Durable LSN lease applies to. */
} REP_LEASE_ENTRY;

/*
 * Old vote info where some fields were not fixed size.
 */
typedef struct {
	u_int32_t	egen;		/* Election generation. */
	int		nsites;		/* Number of sites I've been in
					 * communication with. */
	int		nvotes;		/* Number of votes needed to win. */
	int		priority;	/* My site's priority. */
	u_int32_t	tiebreaker;	/* Tie-breaking quasi-random value. */
} REP_OLD_VOTE_INFO;

typedef struct {
	u_int32_t	egen;		/* Voter's election generation. */
	int		eid;		/* Voter's ID. */
} REP_VTALLY;

/*
 * The REP_THROTTLE_ONLY flag is used to do throttle processing only.
 * If set, it will only allow sending the REP_*_MORE message, but not
 * the normal, non-throttled message.  It is used to support throttling
 * with bulk transfer.
 */
/* Flags for __rep_send_throttle. */
#define	REP_THROTTLE_ONLY	0x0001	/* Send _MORE message only. */

/* Throttled message processing information. */
typedef struct {
	DB_LSN		lsn;		/* LSN of this record. */
	DBT		*data_dbt;	/* DBT of this record. */
	u_int32_t	gbytes;		/* This call's max gbytes sent. */
	u_int32_t	bytes;		/* This call's max bytes sent. */
	u_int32_t	type;		/* Record type. */
} REP_THROTTLE;

/* Bulk processing information. */
/*
 * !!!
 * We use a roff_t for the offset.  We'd really like to use a ptrdiff_t
 * since that really is what it is.  But ptrdiff_t is not portable and
 * doesn't exist everywhere.
 */
typedef struct {
	u_int8_t	*addr;		/* Address of bulk buffer. */
	roff_t		*offp;		/* Ptr to current offset into buffer. */
	u_int32_t	len;		/* Bulk buffer length. */
	u_int32_t	type;		/* Item type in buffer (log, page). */
	DB_LSN		lsn;		/* First LSN in buffer. */
	int		eid;		/* ID of potential recipients. */
#define	BULK_XMIT	0x001		/* Buffer in transit. */
	u_int32_t	*flagsp;	/* Buffer flags. */
} REP_BULK;

/*
 * This structure takes care of representing a transaction.
 * It holds all the records, sorted by page number so that
 * we can obtain locks and apply updates in a deadlock free
 * order.
 */
typedef struct {
	u_int nlsns;
	u_int nalloc;
	DB_LSN *array;
} LSN_COLLECTION;

/*
 * This is used by the page-prep routines to do the lock_vec call to
 * apply the updates for a single transaction or a collection of
 * transactions.
 */
typedef struct {
	int		n;
	DB_LOCKREQ	*reqs;
	DBT		*objs;
} linfo_t;

#if defined(__cplusplus)
}
#endif

#include <contrib/deprecated/bdb/src/dbinc_auto/rep_ext.h>
#endif	/* !_DB_REP_H_ */
