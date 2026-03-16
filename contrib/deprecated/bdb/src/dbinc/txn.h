/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef	_DB_TXN_H_
#define	_DB_TXN_H_

#include <contrib/deprecated/bdb/src/dbinc/xa.h>

#if defined(__cplusplus)
extern "C" {
#endif

/* Operation parameters to the delayed commit processing code. */
typedef enum {
	TXN_CLOSE,		/* Close a DB handle whose close had failed. */
	TXN_REMOVE,		/* Remove a file. */
	TXN_TRADE,		/* Trade lockers. */
	TXN_TRADED,		/* Already traded; downgrade lock. */
	TXN_XTRADE		/* Trade lockers on exclusive db handle. */
} TXN_EVENT_T;

struct __db_txnregion;	typedef struct __db_txnregion DB_TXNREGION;
struct __db_txn_stat_int;
typedef struct __db_txn_stat_int DB_TXN_STAT_INT;
struct __txn_logrec;	typedef struct __txn_logrec DB_TXNLOGREC;

/*
 * !!!
 * TXN_MINIMUM = (DB_LOCK_MAXID + 1) but this makes compilers complain.
 */
#define	TXN_MINIMUM	0x80000000
#define	TXN_MAXIMUM	0xffffffff	/* Maximum number of txn ids. */
#define	TXN_INVALID	0		/* Invalid transaction ID. */

#define	DEF_MAX_TXNS	100		/* Default max transactions. */
#define	TXN_NSLOTS	4		/* Initial slots to hold DB refs */

#define	TXN_PRIORITY_DEFAULT	DB_LOCK_DEFPRIORITY

/*
 * This structure must contain the same fields as the __db_txn_stat struct
 * except for any pointer fields that are filled in only when the struct is
 * being populated for output through the API.
 */
DB_ALIGN8 struct __db_txn_stat_int { /* SHARED */
	u_int32_t st_nrestores;		/* number of restored transactions
					   after recovery. */
#ifndef __TEST_DB_NO_STATISTICS
	DB_LSN	  st_last_ckp;		/* lsn of the last checkpoint */
	time_t	  st_time_ckp;		/* time of last checkpoint */
	u_int32_t st_last_txnid;	/* last transaction id given out */
	u_int32_t st_inittxns;		/* initial txns allocated */
	u_int32_t st_maxtxns;		/* maximum txns possible */
	uintmax_t st_naborts;		/* number of aborted transactions */
	uintmax_t st_nbegins;		/* number of begun transactions */
	uintmax_t st_ncommits;		/* number of committed transactions */
	u_int32_t st_nactive;		/* number of active transactions */
	u_int32_t st_nsnapshot;		/* number of snapshot transactions */
	u_int32_t st_maxnactive;	/* maximum active transactions */
	u_int32_t st_maxnsnapshot;	/* maximum snapshot transactions */
	uintmax_t st_region_wait;	/* Region lock granted after wait. */
	uintmax_t st_region_nowait;	/* Region lock granted without wait. */
	roff_t	  st_regsize;		/* Region size. */
#endif
};

/*
 * Internal data maintained in shared memory for each transaction.
 */
typedef struct __txn_detail {
	u_int32_t txnid;		/* current transaction id
					   used to link free list also */
	pid_t pid;			/* Process owning txn */
	db_threadid_t tid;		/* Thread owning txn */

	DB_LSN	last_lsn;		/* Last LSN written for this txn. */
	DB_LSN	begin_lsn;		/* LSN of begin record. */
	roff_t	parent;			/* Offset of transaction's parent. */
	roff_t	name;			/* Offset of txn name. */
	
	u_int32_t	nlog_dbs;	/* Number of databases used. */
	u_int32_t	nlog_slots;	/* Number of allocated slots. */
	roff_t		log_dbs;	/* Databases used. */

	DB_LSN	read_lsn;		/* Read LSN for MVCC. */
	DB_LSN	visible_lsn;		/* LSN at which this transaction's
					   changes are visible. */
	db_mutex_t	mvcc_mtx;	/* Version mutex. */
	u_int32_t	mvcc_ref;	/* Number of buffers created by this
					   transaction still in cache.  */

	u_int32_t	priority;	/* Deadlock resolution priority. */

	SH_TAILQ_HEAD(__tdkids)	kids;	/* Linked list of child txn detail. */
	SH_TAILQ_ENTRY		klinks;

	/* TXN_{ABORTED, COMMITTED PREPARED, RUNNING} */
	u_int32_t status;		/* status of the transaction */

#define	TXN_DTL_COLLECTED	0x01	/* collected during txn_recover */
#define	TXN_DTL_RESTORED	0x02	/* prepared txn restored */
#define	TXN_DTL_INMEMORY	0x04	/* uses in memory logs */
#define	TXN_DTL_SNAPSHOT	0x08	/* On the list of snapshot txns. */
#define	TXN_DTL_NOWAIT		0x10	/* Don't block on locks. */
	u_int32_t flags;

	SH_TAILQ_ENTRY	links;		/* active/free/snapshot list */

	u_int32_t xa_ref;		/* XA: reference count; number
					   of DB_TXNs reffing this struct */
	/* TXN_XA_{ACTIVE, DEADLOCKED, IDLE, PREPARED, ROLLEDBACK} */
	u_int32_t xa_br_status;		/* status of XA branch */
	u_int8_t gid[DB_GID_SIZE];	/* global transaction id */
	u_int32_t bqual;		/* bqual_length from XID */
	u_int32_t gtrid;		/* gtrid_length from XID */
	int32_t format;			/* XA format */
	roff_t slots[TXN_NSLOTS];	/* Initial DB slot allocation. */
} TXN_DETAIL;

/*
 * DB_TXNMGR --
 *	The transaction manager encapsulates the transaction system.
 */
struct __db_txnmgr {
	/*
	 * These fields need to be protected for multi-threaded support.
	 *
	 * Lock list of active transactions (including the content of each
	 * TXN_DETAIL structure on the list).
	 */
	db_mutex_t mutex;
					/* List of active transactions. */
	TAILQ_HEAD(_chain, __db_txn)	txn_chain;

	u_int32_t n_discards;		/* Number of txns discarded. */

	/* These fields are never updated after creation, so not protected. */
	ENV	*env;			/* Environment. */
	REGINFO	 reginfo;		/* Region information. */
};

/* Macros to lock/unlock the transaction region as a whole. */
#define	TXN_SYSTEM_LOCK(env)						\
	MUTEX_LOCK(env, ((DB_TXNREGION *)				\
	    (env)->tx_handle->reginfo.primary)->mtx_region)
#define	TXN_SYSTEM_UNLOCK(env)						\
	MUTEX_UNLOCK(env, ((DB_TXNREGION *)				\
	    (env)->tx_handle->reginfo.primary)->mtx_region)

/*
 * DB_TXNREGION --
 *	The primary transaction data structure in the shared memory region.
 */
struct __db_txnregion { /* SHARED */
	db_mutex_t	mtx_region;	/* Region mutex. */

	u_int32_t	inittxns;	/* initial number of active TXNs */
	u_int32_t	curtxns;	/* current number of active TXNs */
	u_int32_t	maxtxns;	/* maximum number of active TXNs */
	u_int32_t	last_txnid;	/* last transaction id given out */
	u_int32_t	cur_maxid;	/* current max unused id. */

	db_mutex_t	mtx_ckp;	/* Single thread checkpoints. */
	DB_LSN		last_ckp;	/* lsn of the last checkpoint */
	time_t		time_ckp;	/* time of last checkpoint */

	DB_TXN_STAT_INT	stat;		/* Statistics for txns. */

	u_int32_t n_bulk_txn;		/* Num. bulk txns in progress. */
	u_int32_t n_hotbackup;		/* Num. of outstanding backup notices.*/

#define	TXN_IN_RECOVERY	 0x01		/* environment is being recovered */
	u_int32_t	flags;
					/* active TXN list */
	SH_TAILQ_HEAD(__active) active_txn;
	SH_TAILQ_HEAD(__mvcc) mvcc_txn;
};

/*
 * DB_COMMIT_INFO --
 *	Meta-data uniquely describing a transaction commit across a replication
 *	group.
 */
struct __db_commit_info {
	u_int32_t	version;	/* Stored format version. */
	u_int32_t	gen;		/* Replication master generation. */
	u_int32_t	envid;		/* Unique env ID of master. */
	DB_LSN		lsn;		/* LSN of commit log record. */
};

/*
 * DB_TXNLOGREC --
 *	An in-memory, linked-list copy of a log record.
 */
struct __txn_logrec {
	STAILQ_ENTRY(__txn_logrec) links;/* Linked list. */

	u_int8_t data[1];		/* Log record. */
};

/*
 * Log record types.  Note that these are *not* alphabetical.  This is
 * intentional so that we don't change the meaning of values between
 * software upgrades.
 *
 * EXPECTED, UNEXPECTED, IGNORE, and OK are used in the txnlist functions.
 * Here is an explanation of how the statuses are used.
 *
 * TXN_OK
 *	BEGIN records for transactions found on the txnlist during
 *	OPENFILES (BEGIN records are those with a prev_lsn of 0,0)
 *
 * TXN_COMMIT
 *	Transaction committed and should be rolled forward.
 *
 * TXN_ABORT
 *	This transaction's changes must be undone.  Either there was
 *	never a prepare or commit record for this transaction OR there
 *	was a commit, but we are recovering to a timestamp or particular
 *	LSN and that point is before this transaction's commit.
 *
 * TXN_PREPARE
 *	Prepare record, but no commit record is in the log.
 *
 * TXN_IGNORE
 *	Generic meaning is that this transaction should not be
 *	processed during later recovery passes.  We use it in a
 *	number of different manners:
 *
 *	1. We never saw its BEGIN record.  Therefore, the logs have
 *	   been reclaimed and we *know* that this transaction doesn't
 *	   need to be aborted, because in order for it to be
 *	   reclaimed, there must have been a subsequent checkpoint
 *	   (and any dirty pages for this transaction made it to
 *	   disk).
 *
 *	2. This is a child transaction that created a database.
 *	   For some reason, we don't want to recreate that database
 *	   (i.e., it already exists or some other database created
 *	   after it exists).
 *
 *	3. During recovery open of subdatabases, if the master check fails,
 *	   we use a TXN_IGNORE on the create of the subdb in the nested
 *	   transaction.
 *
 *	4. During a remove, the file with the name being removed isn't
 *	   the file for which we are recovering a remove.
 *
 * TXN_EXPECTED
 *	After a successful open during recovery, we update the
 *	transaction's status to TXN_EXPECTED.  The open was done
 *	in the parent, but in the open log record, we record the
 *	child transaction's ID if we also did a create.  When there
 *	is a valid ID in that field, we use it and mark the child's
 *	status as TXN_EXPECTED (indicating that we don't need to redo
 *	a create for this file).
 *
 *	When recovering a remove, if we don't find or can't open
 *	the file, the child (which does the remove) gets marked
 *	EXPECTED (indicating that we don't need to redo the remove).
 *
 * TXN_UNEXPECTED
 *	During recovery, we attempted an open that should have succeeded
 *	and we got ENOENT, so like with the EXPECTED case, we indicate
 *	in the child that we got the UNEXPECTED return so that we do redo
 *	the creating/deleting operation.
 *
 */
#define	TXN_OK		0
#define	TXN_COMMIT	1
#define	TXN_PREPARE	2
#define	TXN_ABORT	3
#define	TXN_IGNORE	4
#define	TXN_EXPECTED	5
#define	TXN_UNEXPECTED	6

#if defined(__cplusplus)
}
#endif

#include <contrib/deprecated/bdb/src/dbinc_auto/txn_auto.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/txn_ext.h>
#endif /* !_DB_TXN_H_ */
