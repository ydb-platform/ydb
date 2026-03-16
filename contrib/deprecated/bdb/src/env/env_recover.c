/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/fop.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/heap.h"
#include "dbinc/mp.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#ifndef lint
static const char copyright[] =
    "Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.\n";
#endif

static int	__db_log_corrupt __P((ENV *, DB_LSN *));
static int	__env_init_rec_42 __P((ENV *));
static int	__env_init_rec_43 __P((ENV *));
static int	__env_init_rec_46 __P((ENV *));
static int	__env_init_rec_47 __P((ENV *));
static int	__env_init_rec_48 __P((ENV *));
static int	__log_earliest __P((ENV *, DB_LOGC *, int32_t *, DB_LSN *));

static double	__lsn_diff __P((DB_LSN *, DB_LSN *, DB_LSN *, u_int32_t, int));
static int	__log_backup __P((ENV *, DB_LOGC *, DB_LSN *, DB_LSN*));

/*
 * __db_apprec --
 *	Perform recovery.  If max_lsn is non-NULL, then we are trying
 * to synchronize this system up with another system that has a max
 * LSN of max_lsn, so we need to roll back sufficiently far for that
 * to work.  See __log_backup for details.
 *
 * PUBLIC: int __db_apprec __P((ENV *,
 * PUBLIC:     DB_THREAD_INFO *, DB_LSN *, DB_LSN *, int, u_int32_t));
 */
int
__db_apprec(env, ip, max_lsn, trunclsn, update, flags)
	ENV *env;
	DB_THREAD_INFO *ip;
	DB_LSN *max_lsn, *trunclsn;
	int update;
	u_int32_t flags;
{
	DBT data;
	DB_ENV *dbenv;
	DB_LOGC *logc;
	DB_LSN ckp_lsn, first_lsn, last_lsn, lowlsn, lsn, stop_lsn, tlsn;
	DB_LSN *vtrunc_ckp, *vtrunc_lsn;
	DB_TXNHEAD *txninfo;
	DB_TXNREGION *region;
	REGENV *renv;
	REGINFO *infop;
	__txn_ckp_args *ckp_args;
	time_t now, tlow;
	double nfiles;
	u_int32_t hi_txn, log_size, txnid;
	int32_t low;
	int all_recovered, progress, rectype, ret, t_ret;
	char *p, *pass;
	char t1[CTIME_BUFLEN], t2[CTIME_BUFLEN], time_buf[CTIME_BUFLEN];

	COMPQUIET(nfiles, (double)0.001);

	dbenv = env->dbenv;
	logc = NULL;
	ckp_args = NULL;
	hi_txn = TXN_MAXIMUM;
	txninfo = NULL;
	pass = DB_STR_P("initial");
	ZERO_LSN(lsn);

	/*
	 * XXX
	 * Get the log size.  No locking required because we're single-threaded
	 * during recovery.
	 */
	log_size = ((LOG *)env->lg_handle->reginfo.primary)->log_size;

	/*
	 * If we need to, update the env handle timestamp.
	 */
	if (update && REP_ON(env)) {
		infop = env->reginfo;
		renv = infop->primary;
		(void)time(&renv->rep_timestamp);
	}

	/* Set in-recovery flags. */
	F_SET(env->lg_handle, DBLOG_RECOVER);
	region = env->tx_handle->reginfo.primary;
	F_SET(region, TXN_IN_RECOVERY);

	/* Allocate a cursor for the log. */
	if ((ret = __log_cursor(env, &logc)) != 0)
		goto err;

	/*
	 * If the user is specifying recovery to a particular point in time
	 * or to a particular LSN, find the point to start recovery from.
	 */
	ZERO_LSN(lowlsn);
	if (max_lsn != NULL) {
		if ((ret = __log_backup(env, logc, max_lsn, &lowlsn)) != 0)
			goto err;
	} else if (dbenv->tx_timestamp != 0) {
		if ((ret = __log_earliest(env, logc, &low, &lowlsn)) != 0)
			goto err;
		if ((int32_t)dbenv->tx_timestamp < low) {
			t1[sizeof(t1) - 1] = '\0';
			(void)strncpy(t1, __os_ctime(
			    &dbenv->tx_timestamp, time_buf), sizeof(t1) - 1);
			if ((p = strchr(t1, '\n')) != NULL)
				*p = '\0';

			t2[sizeof(t2) - 1] = '\0';
			tlow = (time_t)low;
			(void)strncpy(t2, __os_ctime(
			    &tlow, time_buf), sizeof(t2) - 1);
			if ((p = strchr(t2, '\n')) != NULL)
				*p = '\0';

			__db_errx(env, DB_STR_A("1509",
		    "Invalid recovery timestamp %s; earliest time is %s",
			    "%s %s"), t1, t2);
			ret = EINVAL;
			goto err;
		}
	}

	/*
	 * Recovery is done in three passes:
	 * Pass #0:
	 *	We need to find the position from which we will open files.
	 *	We need to open files beginning with the earlier of the
	 *	most recent checkpoint LSN and a checkpoint LSN before the
	 *	recovery timestamp, if specified.  We need to be before the
	 *	most recent checkpoint LSN because we are going to collect
	 *	information about which transactions were begun before we
	 *	start rolling forward.  Those that were should never be undone
	 *	because queue cannot use LSNs to determine what operations can
	 *	safely be aborted and it cannot rollback operations in
	 *	transactions for which there may be records not processed
	 *	during recovery.  We need to consider earlier points in time
	 *	in case we are recovering to a particular timestamp.
	 *
	 * Pass #1:
	 *	Read forward through the log from the position found in pass 0
	 *	opening and closing files, and recording transactions for which
	 *	we've seen their first record (the transaction's prev_lsn is
	 *	0,0).  At the end of this pass, we know all transactions for
	 *	which we've seen begins and we have the "current" set of files
	 *	open.
	 *
	 * Pass #2:
	 *	Read backward through the log undoing any uncompleted TXNs.
	 *	There are four cases:
	 *	    1.  If doing catastrophic recovery, we read to the
	 *		beginning of the log
	 *	    2.  If we are doing normal reovery, then we have to roll
	 *		back to the most recent checkpoint LSN.
	 *	    3.  If we are recovering to a point in time, then we have
	 *		to roll back to the checkpoint whose ckp_lsn is earlier
	 *		than the specified time.  __log_earliest will figure
	 *		this out for us.
	 *	    4.	If we are recovering back to a particular LSN, then
	 *		we have to roll back to the checkpoint whose ckp_lsn
	 *		is earlier than the max_lsn.  __log_backup will figure
	 *		that out for us.
	 *	In case 2, "uncompleted TXNs" include all those who committed
	 *	after the user's specified timestamp.
	 *
	 * Pass #3:
	 *	Read forward through the log from the LSN found in pass #2,
	 *	redoing any committed TXNs (which committed after any user-
	 *	specified rollback point).  During this pass, checkpoint
	 *	file information is ignored, and file openings and closings
	 *	are redone.
	 *
	 * ckp_lsn   -- lsn of the last checkpoint or the first in the log.
	 * first_lsn -- the lsn where the forward passes begin.
	 * last_lsn  -- the last lsn in the log, used for feedback
	 * lowlsn    -- the lsn we are rolling back to, if we are recovering
	 *		to a point in time.
	 * lsn       -- temporary use lsn.
	 * stop_lsn  -- the point at which forward roll should stop
	 */

	/*
	 * Find out the last lsn, so that we can estimate how far along we
	 * are in recovery.  This will help us determine how much log there
	 * is between the first LSN that we're going to be working with and
	 * the last one.  We assume that each of the three phases takes the
	 * same amount of time (a false assumption) and then use the %-age
	 * of the amount of log traversed to figure out how much of the
	 * pass we've accomplished.
	 *
	 * If we can't find any log records, we're kind of done.
	 */
#ifdef UMRW
	ZERO_LSN(last_lsn);
#endif
	memset(&data, 0, sizeof(data));
	/*
	 * Pass #0
	 * Find the LSN from which we begin OPENFILES.
	 *
	 * If this is a catastrophic recovery, or if no checkpoint exists
	 * in the log, the LSN is the first LSN in the log.
	 *
	 * Otherwise, it is the minimum of (1) the LSN in the last checkpoint
	 * and (2) the LSN in the checkpoint before any specified recovery
	 * timestamp or max_lsn.
	 */
	/*
	 * Get the first LSN in the log; it's an initial default
	 * even if this is not a catastrophic recovery.
	 */
	if ((ret = __logc_get(logc, &ckp_lsn, &data, DB_FIRST)) != 0) {
		if (ret == DB_NOTFOUND)
			ret = 0;
		else
			__db_errx(env, DB_STR("1510",
			    "First log record not found"));
		goto err;
	}
	first_lsn = ckp_lsn;

	if (!LF_ISSET(DB_RECOVER_FATAL)) {
		if ((ret = __txn_getckp(env, &ckp_lsn)) == 0 &&
		    (ret = __logc_get(logc, &ckp_lsn, &data, DB_SET)) == 0) {
			/* We have a recent checkpoint.  This is LSN (1). */
			if ((ret = __txn_ckp_read(env,
			    data.data, &ckp_args)) != 0) {
				__db_errx(env, DB_STR_A("1511",
				    "Invalid checkpoint record at [%ld][%ld]",
				    "%ld %ld"), (u_long)ckp_lsn.file,
				    (u_long)ckp_lsn.offset);
				goto err;
			}
			first_lsn = ckp_args->ckp_lsn;
			__os_free(env, ckp_args);
		}

		/*
		 * If LSN (2) exists, use it if it's before LSN (1).
		 * (If LSN (1) doesn't exist, first_lsn is the
		 * beginning of the log, so will "win" this check.)
		 *
		 * XXX
		 * In the recovery-to-a-timestamp case, lowlsn is chosen by
		 * __log_earliest, and is the checkpoint LSN of the
		 * *earliest* checkpoint in the unreclaimed log.  I
		 * (krinsky) believe that we could optimize this by looking
		 * instead for the LSN of the *latest* checkpoint before
		 * the timestamp of interest, but I'm not sure that this
		 * is worth doing right now.  (We have to look for lowlsn
		 * and low anyway, to make sure the requested timestamp is
		 * somewhere in the logs we have, and all that's required
		 * is that we pick *some* checkpoint after the beginning of
		 * the logs and before the timestamp.
		 */
		if ((dbenv->tx_timestamp != 0 || max_lsn != NULL) &&
		    LOG_COMPARE(&lowlsn, &first_lsn) < 0) {
			first_lsn = lowlsn;
		}
	}

	if ((ret = __logc_get(logc, &last_lsn, &data, DB_LAST)) != 0) {
		if (ret == DB_NOTFOUND)
			ret = 0;
		else
			__db_errx(env, DB_STR("1512",
			    "Last log record not found"));
		goto err;
	}

	rectype = 0;
	txnid = 0;
	do {
		if (LOG_COMPARE(&lsn, &first_lsn) == 0)
			break;
		/* check if we have a recycle record. */
		if (rectype != DB___txn_recycle)
			LOGCOPY_32(env, &rectype, data.data);
		/* txnid is after rectype, which is a u_int32. */
		LOGCOPY_32(env, &txnid,
		    (u_int8_t *)data.data + sizeof(u_int32_t));

		if (txnid != 0)
			break;
	} while ((ret = __logc_get(logc, &lsn, &data, DB_PREV)) == 0);

	/*
	 * There are no transactions, so there is nothing to do unless
	 * we're recovering to an LSN.  If we are, we need to proceed since
	 * we'll still need to do a vtruncate based on information we haven't
	 * yet collected.
	 */
	if (ret == DB_NOTFOUND)
		ret = 0;
	else if (ret != 0)
		goto err;

	hi_txn = txnid;

	/* Get the record at first_lsn. */
	if ((ret = __logc_get(logc, &first_lsn, &data, DB_SET)) != 0) {
		__db_errx(env, DB_STR_A("1513",
		    "Checkpoint LSN record [%ld][%ld] not found", "%ld %ld"),
		    (u_long)first_lsn.file, (u_long)first_lsn.offset);
		goto err;
	}

	if (dbenv->db_feedback != NULL) {
		if (last_lsn.file == first_lsn.file)
			nfiles = (double)
			    (last_lsn.offset - first_lsn.offset) / log_size;
		else
			nfiles = (double)(last_lsn.file - first_lsn.file) +
			    (double)((log_size - first_lsn.offset) +
			    last_lsn.offset) / log_size;
		/* We are going to divide by nfiles; make sure it isn't 0. */
		if (nfiles < 0.001)
			nfiles = 0.001;
	}

	/* Find a low txnid. */
	ret = 0;
	if (hi_txn != 0) do {
		/* txnid is after rectype, which is a u_int32. */
		LOGCOPY_32(env, &txnid,
		    (u_int8_t *)data.data + sizeof(u_int32_t));

		if (txnid != 0)
			break;
	} while ((ret = __logc_get(logc, &lsn, &data, DB_NEXT)) == 0);

	/*
	 * There are no transactions and we're not recovering to an LSN (see
	 * above), so there is nothing to do.
	 */
	if (ret == DB_NOTFOUND) {
		if (LOG_COMPARE(&lsn, &last_lsn) != 0)
			ret = __db_log_corrupt(env, &lsn);
		else
			ret = 0;
	}

	/* Reset to the first lsn. */
	if (ret != 0 ||
	    (ret = __logc_get(logc, &first_lsn, &data, DB_SET)) != 0)
		goto err;

	/* Initialize the transaction list. */
	if ((ret = __db_txnlist_init(env, ip,
	    txnid, hi_txn, max_lsn, &txninfo)) != 0)
		goto err;

	/*
	 * Pass #1
	 * Run forward through the log starting at the first relevant lsn.
	 */
	if ((ret = __env_openfiles(env, logc,
	    txninfo, &data, &first_lsn, &last_lsn, nfiles, 1)) != 0)
		goto err;

	/* If there were no transactions, then we can bail out early. */
	if (hi_txn == 0 && max_lsn == NULL) {
		lsn = last_lsn;
		goto done;
	}

	/*
	 * Pass #2.
	 *
	 * We used first_lsn to tell us how far back we need to recover,
	 * use it here.
	 */
	if (FLD_ISSET(dbenv->verbose, DB_VERB_RECOVERY))
		__db_msg(env, DB_STR_A("1514",
		    "Recovery starting from [%lu][%lu]", "%lu %lu"),
		    (u_long)first_lsn.file, (u_long)first_lsn.offset);

	pass = DB_STR_P("backward");
	for (ret = __logc_get(logc, &lsn, &data, DB_LAST);
	    ret == 0 && LOG_COMPARE(&lsn, &first_lsn) >= 0;
	    ret = __logc_get(logc, &lsn, &data, DB_PREV)) {
		if (dbenv->db_feedback != NULL) {
			progress = 34 + (int)(33 * (__lsn_diff(&first_lsn,
			    &last_lsn, &lsn, log_size, 0) / nfiles));
			dbenv->db_feedback(dbenv, DB_RECOVER, progress);
		}

		tlsn = lsn;
		ret = __db_dispatch(env, &env->recover_dtab,
		    &data, &tlsn, DB_TXN_BACKWARD_ROLL, txninfo);
		if (ret != 0) {
			if (ret != DB_TXN_CKP)
				goto msgerr;
			else
				ret = 0;
		}
	}
	if (ret == DB_NOTFOUND) {
		if (LOG_COMPARE(&lsn, &first_lsn) > 0)
			ret = __db_log_corrupt(env, &lsn);
		else
			ret = 0;
	}
	if (ret != 0)
		goto err;

	/*
	 * Pass #3.  If we are recovering to a timestamp or to an LSN,
	 * we need to make sure that we don't roll-forward beyond that
	 * point because there may be non-transactional operations (e.g.,
	 * closes that would fail).  The last_lsn variable is used for
	 * feedback calculations, but use it to set an initial stopping
	 * point for the forward pass, and then reset appropriately to
	 * derive a real stop_lsn that tells how far the forward pass
	 * should go.
	 */
	pass = DB_STR_P("forward");
	stop_lsn = last_lsn;
	if (max_lsn != NULL || dbenv->tx_timestamp != 0)
		stop_lsn = ((DB_TXNHEAD *)txninfo)->maxlsn;

	for (ret = __logc_get(logc, &lsn, &data, DB_NEXT);
	    ret == 0; ret = __logc_get(logc, &lsn, &data, DB_NEXT)) {
		if (dbenv->db_feedback != NULL) {
			progress = 67 + (int)(33 * (__lsn_diff(&first_lsn,
			    &last_lsn, &lsn, log_size, 1) / nfiles));
			dbenv->db_feedback(dbenv, DB_RECOVER, progress);
		}

		tlsn = lsn;
		ret = __db_dispatch(env, &env->recover_dtab,
		    &data, &tlsn, DB_TXN_FORWARD_ROLL, txninfo);
		if (ret != 0) {
			if (ret != DB_TXN_CKP)
				goto msgerr;
			else
				ret = 0;
		}
		/*
		 * If we are recovering to a timestamp or an LSN,
		 * we need to make sure that we don't try to roll
		 * forward beyond the soon-to-be end of log.
		 */
		if (LOG_COMPARE(&lsn, &stop_lsn) >= 0)
			break;

	}
	if (ret == DB_NOTFOUND)
		ret = __db_log_corrupt(env, &lsn);
	if (ret != 0)
		goto err;

	if (max_lsn == NULL)
		region->last_txnid = ((DB_TXNHEAD *)txninfo)->maxid;

done:
	/* We are going to truncate, so we'd best close the cursor. */
	if (logc != NULL) {
		if ((ret = __logc_close(logc)) != 0)
			goto err;
		logc = NULL;
	}
	/*
	 * Also flush the cache before truncating the log. It's recovery,
	 * ignore any application max-write configuration.
	 */
	if ((ret = __memp_sync_int(env,
	    NULL, 0, DB_SYNC_CACHE | DB_SYNC_SUPPRESS_WRITE, NULL, NULL)) != 0)
		goto err;
	if (dbenv->tx_timestamp != 0) {
		/* Run recovery up to this timestamp. */
		region->last_ckp = ((DB_TXNHEAD *)txninfo)->ckplsn;
		vtrunc_lsn = &((DB_TXNHEAD *)txninfo)->maxlsn;
		vtrunc_ckp = &((DB_TXNHEAD *)txninfo)->ckplsn;
	} else if (max_lsn != NULL) {
		/* This is a HA client syncing to the master. */
		if (!IS_ZERO_LSN(((DB_TXNHEAD *)txninfo)->ckplsn))
			region->last_ckp = ((DB_TXNHEAD *)txninfo)->ckplsn;
		else if ((ret =
		    __txn_findlastckp(env, &region->last_ckp, max_lsn)) != 0)
			goto err;
		vtrunc_lsn = max_lsn;
		vtrunc_ckp = &((DB_TXNHEAD *)txninfo)->ckplsn;
	} else {
		/*
		 * The usual case: we recovered the whole (valid) log; clear
		 * out any partial record after the recovery point.
		 */
		vtrunc_lsn = &lsn;
		vtrunc_ckp = &region->last_ckp;
	}
	if ((ret = __log_vtruncate(env, vtrunc_lsn, vtrunc_ckp, trunclsn)) != 0)
		goto err;

	/* If we had no txns, figure out if we need a checkpoint. */
	if (hi_txn == 0 && __dbreg_log_nofiles(env))
		LF_SET(DB_NO_CHECKPOINT);
	/*
	 * Usually we close all files at the end of recovery, unless there are
	 * prepared transactions or errors in the checkpoint.
	 */
	all_recovered = region->stat.st_nrestores == 0;
	/*
	 * Log a checkpoint here so subsequent recoveries can skip what's been
	 * done; this is unnecessary for HA rep clients, as they do not write
	 * log records.
	 */
	if (max_lsn == NULL &&	!LF_ISSET(DB_NO_CHECKPOINT) &&
	    (ret = __txn_checkpoint(env,
		0, 0, DB_CKP_INTERNAL | DB_FORCE)) != 0) {
		/*
		 * If there was no space for the checkpoint or flushing db
		 * pages we can still bring the environment up, if only for
		 * read-only access. We must not close the open files because a
		 * subsequent recovery might still need to redo this portion
		 * of the log [#18590].
		 */
		if (max_lsn == NULL && ret == ENOSPC) {
			if (FLD_ISSET(dbenv->verbose, DB_VERB_RECOVERY))
				__db_msg(env, DB_STR_A("1515",
		    "Recovery continuing after non-fatal checkpoint error: %s",
				    "%s"), db_strerror(ret));
			all_recovered = 0;
		}
		else
			goto err;
	}

	if (all_recovered ) {
		/* Close all the db files that are open. */
		if ((ret = __dbreg_close_files(env, 0)) != 0)
			goto err;
	} else {
		if ((ret = __dbreg_mark_restored(env)) != 0)
			goto err;
		F_SET(env->lg_handle, DBLOG_OPENFILES);
	}

	if (max_lsn != NULL) {
		/*
		 * Now we need to open files that should be open in order for
		 * client processing to continue.  However, since we've
		 * truncated the log, we need to recompute from where the
		 * openfiles pass should begin.
		 */
		if ((ret = __log_cursor(env, &logc)) != 0)
			goto err;
		if ((ret =
		    __logc_get(logc, &first_lsn, &data, DB_FIRST)) != 0) {
			if (ret == DB_NOTFOUND)
				ret = 0;
			else
				__db_errx(env, DB_STR("1516",
				    "First log record not found"));
			goto err;
		}
		if ((ret = __txn_getckp(env, &first_lsn)) == 0 &&
		    (ret = __logc_get(logc, &first_lsn, &data, DB_SET)) == 0) {
			/* We have a recent checkpoint.  This is LSN (1). */
			if ((ret = __txn_ckp_read(env,
			    data.data, &ckp_args)) != 0) {
				__db_errx(env, DB_STR_A("1517",
				    "Invalid checkpoint record at [%ld][%ld]",
				    "%ld %ld"), (u_long)first_lsn.file,
				    (u_long)first_lsn.offset);
				goto err;
			}
			first_lsn = ckp_args->ckp_lsn;
			__os_free(env, ckp_args);
		}
		if ((ret = __logc_get(logc, &first_lsn, &data, DB_SET)) != 0)
			goto err;
		if ((ret = __env_openfiles(env, logc,
		    txninfo, &data, &first_lsn, max_lsn, nfiles, 1)) != 0)
			goto err;
	} else if (all_recovered) {
		/*
		 * If there are no transactions that need resolution, whether
		 * because they are prepared or because recovery will need to
		 * process them, we need to reset the transaction ID space and
		 * log this fact.
		 */
		if ((rectype != DB___txn_recycle || hi_txn != 0) &&
		    (ret = __txn_reset(env)) != 0)
			goto err;
	} else {
		if ((ret = __txn_recycle_id(env, 0)) != 0)
			goto err;
	}

	if (FLD_ISSET(dbenv->verbose, DB_VERB_RECOVERY)) {
		(void)time(&now);
		__db_msg(env, DB_STR_A("1518",
		    "Recovery complete at %.24s", "%.24s"),
		    __os_ctime(&now, time_buf));
		__db_msg(env, DB_STR_A("1519",
		    "Maximum transaction ID %lx recovery checkpoint [%lu][%lu]",
		    "%lx %lu %lu"), (u_long)(txninfo == NULL ?
		    TXN_MINIMUM : ((DB_TXNHEAD *)txninfo)->maxid),
		    (u_long)region->last_ckp.file,
		    (u_long)region->last_ckp.offset);
	}

	if (0) {
msgerr:		__db_errx(env, DB_STR_A("1520",
		    "Recovery function for LSN %lu %lu failed on %s pass",
		    "%lu %lu %s"), (u_long)lsn.file, (u_long)lsn.offset, pass);
	}

err:	if (logc != NULL && (t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (txninfo != NULL)
		__db_txnlist_end(env, txninfo);

	dbenv->tx_timestamp = 0;

	F_CLR(env->lg_handle, DBLOG_RECOVER);
	F_CLR(region, TXN_IN_RECOVERY);

	return (ret);
}

/*
 * Figure out how many logfiles we have processed.  If we are moving
 * forward (is_forward != 0), then we're computing current - low.  If
 * we are moving backward, we are computing high - current.  max is
 * the number of bytes per logfile.
 */
static double
__lsn_diff(low, high, current, max, is_forward)
	DB_LSN *low, *high, *current;
	u_int32_t max;
	int is_forward;
{
	double nf;

	/*
	 * There are three cases in each direction.  If you are in the
	 * same file, then all you need worry about is the difference in
	 * offsets.  If you are in different files, then either your offsets
	 * put you either more or less than the integral difference in the
	 * number of files -- we need to handle both of these.
	 */
	if (is_forward) {
		if (current->file == low->file)
			nf = (double)(current->offset - low->offset) / max;
		else if (current->offset < low->offset)
			nf = (double)((current->file - low->file) - 1) +
			    (double)((max - low->offset) + current->offset) /
			    max;
		else
			nf = (double)(current->file - low->file) +
			    (double)(current->offset - low->offset) / max;
	} else {
		if (current->file == high->file)
			nf = (double)(high->offset - current->offset) / max;
		else if (current->offset > high->offset)
			nf = (double)((high->file - current->file) - 1) +
			    (double)
			    ((max - current->offset) + high->offset) / max;
		else
			nf = (double)(high->file - current->file) +
			    (double)(high->offset - current->offset) / max;
	}
	return (nf);
}

/*
 * __log_backup --
 *
 * This is used to find the earliest log record to process when a client
 * is trying to sync up with a master whose max LSN is less than this
 * client's max lsn; we want to roll back everything after that.
 *
 * Find the latest checkpoint whose ckp_lsn is less than the max lsn.
 */
static int
__log_backup(env, logc, max_lsn, start_lsn)
	ENV *env;
	DB_LOGC *logc;
	DB_LSN *max_lsn, *start_lsn;
{
	DBT data;
	DB_LSN lsn;
	__txn_ckp_args *ckp_args;
	int ret;

	memset(&data, 0, sizeof(data));
	ckp_args = NULL;

	if ((ret = __txn_getckp(env, &lsn)) != 0)
		goto err;
	while ((ret = __logc_get(logc, &lsn, &data, DB_SET)) == 0) {
		if ((ret = __txn_ckp_read(env, data.data, &ckp_args)) != 0)
			return (ret);
		/*
		 * Follow checkpoints through the log until
		 * we find one with a ckp_lsn less than
		 * or equal max_lsn.
		 */
		if (LOG_COMPARE(&ckp_args->ckp_lsn, max_lsn) <= 0) {
			*start_lsn = ckp_args->ckp_lsn;
			break;
		}

		lsn = ckp_args->last_ckp;
		/*
		 * If there are no more checkpoints behind us, we're
		 * done.  Break with DB_NOTFOUND.
		 */
		if (IS_ZERO_LSN(lsn)) {
			ret = DB_NOTFOUND;
			break;
		}
		__os_free(env, ckp_args);
		ckp_args = NULL;
	}

	if (ckp_args != NULL)
		__os_free(env, ckp_args);
	/*
	 * If we walked back through all the checkpoints,
	 * set the cursor on the first log record.
	 */
err:	if (IS_ZERO_LSN(*start_lsn) && (ret == 0 || ret == DB_NOTFOUND))
		ret = __logc_get(logc, start_lsn, &data, DB_FIRST);
	return (ret);
}

/*
 * __log_earliest --
 *
 * Return the earliest recovery point for the log files present.  The
 * earliest recovery time is the time stamp of the first checkpoint record
 * whose checkpoint LSN is greater than the first LSN we process.
 */
static int
__log_earliest(env, logc, lowtime, lowlsn)
	ENV *env;
	DB_LOGC *logc;
	int32_t *lowtime;
	DB_LSN *lowlsn;
{
	__txn_ckp_args *ckpargs;
	DB_LSN first_lsn, lsn;
	DBT data;
	u_int32_t rectype;
	int cmp, ret;

	memset(&data, 0, sizeof(data));

	/*
	 * Read forward through the log looking for the first checkpoint
	 * record whose ckp_lsn is greater than first_lsn.
	 */
	for (ret = __logc_get(logc, &first_lsn, &data, DB_FIRST);
	    ret == 0; ret = __logc_get(logc, &lsn, &data, DB_NEXT)) {
		LOGCOPY_32(env, &rectype, data.data);
		if (rectype != DB___txn_ckp)
			continue;
		if ((ret =
		    __txn_ckp_read(env, data.data, &ckpargs)) == 0) {
			cmp = LOG_COMPARE(&ckpargs->ckp_lsn, &first_lsn);
			*lowlsn = ckpargs->ckp_lsn;
			*lowtime = ckpargs->timestamp;

			__os_free(env, ckpargs);
			if (cmp >= 0)
				break;
		}
	}

	return (ret);
}

/*
 * __env_openfiles --
 * Perform the pass of recovery that opens files.  This is used
 * both during regular recovery and an initial call to txn_recover (since
 * we need files open in order to abort prepared, but not yet committed
 * transactions).
 *
 * See the comments in db_apprec for a detailed description of the
 * various recovery passes.
 *
 * If we are not doing feedback processing (i.e., we are doing txn_recover
 * processing and in_recovery is zero), then last_lsn can be NULL.
 *
 * PUBLIC: int __env_openfiles __P((ENV *,
 * PUBLIC:     DB_LOGC *, void *, DBT *, DB_LSN *, DB_LSN *, double, int));
 */
int
__env_openfiles(env, logc, txninfo,
    data, open_lsn, last_lsn, nfiles, in_recovery)
	ENV *env;
	DB_LOGC *logc;
	void *txninfo;
	DBT *data;
	DB_LSN *open_lsn, *last_lsn;
	double nfiles;
	int in_recovery;
{
	DB_ENV *dbenv;
	DB_LSN lsn, tlsn;
	u_int32_t log_size;
	int progress, ret;

	dbenv = env->dbenv;

	/*
	 * XXX
	 * Get the log size.  No locking required because we're single-threaded
	 * during recovery.
	 */
	log_size = ((LOG *)env->lg_handle->reginfo.primary)->log_size;

	lsn = *open_lsn;
	for (;;) {
		if (in_recovery && dbenv->db_feedback != NULL) {
			DB_ASSERT(env, last_lsn != NULL);
			progress = (int)(33 * (__lsn_diff(open_lsn,
			   last_lsn, &lsn, log_size, 1) / nfiles));
			dbenv->db_feedback(dbenv, DB_RECOVER, progress);
		}

		tlsn = lsn;
		ret = __db_dispatch(env, &env->recover_dtab, data, &tlsn,
		    in_recovery ? DB_TXN_OPENFILES : DB_TXN_POPENFILES,
		    txninfo);
		if (ret != 0 && ret != DB_TXN_CKP) {
			__db_errx(env, DB_STR_A("1521",
			    "Recovery function for LSN %lu %lu failed",
			    "%lu %lu"), (u_long)lsn.file, (u_long)lsn.offset);
			break;
		}
		if ((ret = __logc_get(logc, &lsn, data, DB_NEXT)) != 0) {
			if (ret == DB_NOTFOUND) {
				if (last_lsn != NULL &&
				   LOG_COMPARE(&lsn, last_lsn) != 0)
					ret = __db_log_corrupt(env, &lsn);
				else
					ret = 0;
			}
			break;
		}
	}

	return (ret);
}

static int
__db_log_corrupt(env, lsnp)
	ENV *env;
	DB_LSN *lsnp;
{
	__db_errx(env, DB_STR_A("1522",
	    "Log file corrupt at LSN: [%lu][%lu]", "%lu %lu"),
	    (u_long)lsnp->file, (u_long)lsnp->offset);
	return (EINVAL);
}

/*
 * __env_init_rec --
 *
 * PUBLIC: int __env_init_rec __P((ENV *, u_int32_t));
 */
int
__env_init_rec(env, version)
	ENV *env;
	u_int32_t version;
{
	int ret;

	/*
	 * We need to prime the recovery table with the current recovery
	 * functions.  Then we overwrite only specific entries based on
	 * each previous version we support.
	 */
	if ((ret = __bam_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __crdel_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __db_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __dbreg_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __fop_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __ham_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __heap_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __qam_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __repmgr_init_recover(env, &env->recover_dtab)) != 0)
		goto err;
	if ((ret = __txn_init_recover(env, &env->recover_dtab)) != 0)
		goto err;

	/*
	 * After installing all the current recovery routines, we want to
	 * override them with older versions if we are reading a down rev
	 * log (from a downrev replication master).  If a log record is
	 * changed then we must use the previous version for all older
	 * logs.  If a record is changed in multiple revisions then the
	 * oldest revision that applies must be used.  Therefore we override
	 * the recovery functions in reverse log version order.
	 */
	/*
	 * DB_LOGVERSION_53 is a strict superset of DB_LOGVERSION_50.
	 * So, only check > DB_LOGVERSION_48p2.  If/When log records are
	 * altered, the condition below will need to change.
	 */
	if (version > DB_LOGVERSION_48p2)
		goto done;
	if ((ret = __env_init_rec_48(env)) != 0)
		goto err;
	/*
	 * Patch 2 added __db_pg_trunc but did not replace any log records
	 * so we want to override the same functions as in the original release.
	 */
	if (version >= DB_LOGVERSION_48)
		goto done;
	if ((ret = __env_init_rec_47(env)) != 0)
		goto err;
	if (version == DB_LOGVERSION_47)
		goto done;
	if ((ret = __env_init_rec_46(env)) != 0)
		goto err;
	/*
	 * There are no log record/recovery differences between 4.4 and 4.5.
	 * The log version changed due to checksum.  There are no log recovery
	 * differences between 4.5 and 4.6.  The name of the rep_gen in
	 * txn_checkpoint changed (to spare, since we don't use it anymore).
	 */
	if (version >= DB_LOGVERSION_44)
		goto done;
	if ((ret = __env_init_rec_43(env)) != 0)
		goto err;
	if (version == DB_LOGVERSION_43)
		goto done;
	if (version != DB_LOGVERSION_42) {
		__db_errx(env, DB_STR_A("1523", "Unknown version %lu",
		    "%lu"), (u_long)version);
		ret = EINVAL;
		goto err;
	}
	ret = __env_init_rec_42(env);

done:
err:	return (ret);
}

static int
__env_init_rec_42(env)
	ENV *env;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_relink_42_recover, DB___db_relink_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_pg_alloc_42_recover, DB___db_pg_alloc_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_pg_free_42_recover, DB___db_pg_free_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_pg_freedata_42_recover, DB___db_pg_freedata_42)) != 0)
		goto err;
#ifdef HAVE_HASH
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __ham_metagroup_42_recover, DB___ham_metagroup_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __ham_groupalloc_42_recover, DB___ham_groupalloc_42)) != 0)
		goto err;
#endif
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __txn_ckp_42_recover, DB___txn_ckp_42)) != 0)
		goto err;
err:
	return (ret);
}

static int
__env_init_rec_43(env)
	ENV *env;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __bam_relink_43_recover, DB___bam_relink_43)) != 0)
		goto err;
	/*
	 * We want to use the 4.2-based txn_regop record.
	 */
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __txn_regop_42_recover, DB___txn_regop_42)) != 0)
		goto err;
err:
	return (ret);
}

static int
__env_init_rec_46(env)
	ENV *env;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __bam_merge_44_recover, DB___bam_merge_44)) != 0)
		goto err;

err:	return (ret);
}

static int
__env_init_rec_47(env)
	ENV *env;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __bam_split_42_recover, DB___bam_split_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_pg_sort_44_recover, DB___db_pg_sort_44)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __fop_create_42_recover, DB___fop_create_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __fop_write_42_recover, DB___fop_write_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __fop_rename_42_recover, DB___fop_rename_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __fop_rename_noundo_46_recover, DB___fop_rename_noundo_46)) != 0)
		goto err;

err:
	return (ret);
}

static int
__env_init_rec_48(env)
	ENV *env;
{
	int ret;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_pg_sort_44_recover, DB___db_pg_sort_44)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_addrem_42_recover, DB___db_addrem_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __db_big_42_recover, DB___db_big_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __bam_split_48_recover, DB___bam_split_48)) != 0)
		goto err;
#ifdef HAVE_HASH
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __ham_insdel_42_recover, DB___ham_insdel_42)) != 0)
		goto err;
	if ((ret = __db_add_recovery_int(env, &env->recover_dtab,
	    __ham_replace_42_recover, DB___ham_replace_42)) != 0)
		goto err;
#endif
err:
	return (ret);
}
