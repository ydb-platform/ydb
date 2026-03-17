/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/hmac.h"
#include "dbinc/log.h"
#include "dbinc/hash.h"

typedef enum { L_ALREADY, L_ACQUIRED, L_NONE } RLOCK;

static int __logc_close_pp __P((DB_LOGC *, u_int32_t));
static int __logc_get_pp __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
static int __logc_get_int __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
static int __logc_hdrchk __P((DB_LOGC *, DB_LSN *, HDR *, int *));
static int __logc_incursor __P((DB_LOGC *, DB_LSN *, HDR *, u_int8_t **));
static int __logc_inregion __P((DB_LOGC *,
	       DB_LSN *, RLOCK *, DB_LSN *, HDR *, u_int8_t **, int *));
static int __logc_io __P((DB_LOGC *,
	       u_int32_t, u_int32_t, void *, size_t *, int *));
static int __logc_ondisk __P((DB_LOGC *,
	       DB_LSN *, DB_LSN *, u_int32_t, HDR *, u_int8_t **, int *));
static int __logc_set_maxrec __P((DB_LOGC *, char *));
static int __logc_shortread __P((DB_LOGC *, DB_LSN *, int));
static int __logc_version_pp __P((DB_LOGC *, u_int32_t *, u_int32_t));

/*
 * __log_cursor_pp --
 *	ENV->log_cursor
 *
 * PUBLIC: int __log_cursor_pp __P((DB_ENV *, DB_LOGC **, u_int32_t));
 */
int
__log_cursor_pp(dbenv, logcp, flags)
	DB_ENV *dbenv;
	DB_LOGC **logcp;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = dbenv->env;

	ENV_REQUIRES_CONFIG(env,
	    env->lg_handle, "DB_ENV->log_cursor", DB_INIT_LOG);

	/* Validate arguments. */
	if ((ret = __db_fchk(env, "DB_ENV->log_cursor", flags, 0)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__log_cursor(env, logcp)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __log_cursor --
 *	Create a log cursor.
 *
 * PUBLIC: int __log_cursor __P((ENV *, DB_LOGC **));
 */
int
__log_cursor(env, logcp)
	ENV *env;
	DB_LOGC **logcp;
{
	DB_LOGC *logc;
	int ret;

	*logcp = NULL;

	/* Allocate memory for the cursor. */
	if ((ret = __os_calloc(env, 1, sizeof(DB_LOGC), &logc)) != 0)
		return (ret);

	logc->bp_size = LG_CURSOR_BUF_SIZE;
	/*
	 * Set this to something positive.
	 */
	logc->bp_maxrec = MEGABYTE;
	if ((ret = __os_malloc(env, logc->bp_size, &logc->bp)) != 0) {
		__os_free(env, logc);
		return (ret);
	}

	logc->env = env;
	logc->close = __logc_close_pp;
	logc->get = __logc_get_pp;
	logc->version = __logc_version_pp;

	*logcp = logc;
	return (0);
}

/*
 * __logc_close_pp --
 *	DB_LOGC->close pre/post processing.
 */
static int
__logc_close_pp(logc, flags)
	DB_LOGC *logc;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = logc->env;

	if ((ret = __db_fchk(env, "DB_LOGC->close", flags, 0)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__logc_close(logc)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __logc_close --
 *	DB_LOGC->close.
 *
 * PUBLIC: int __logc_close __P((DB_LOGC *));
 */
int
__logc_close(logc)
	DB_LOGC *logc;
{
	ENV *env;

	env = logc->env;

	if (logc->fhp != NULL) {
		(void)__os_closehandle(env, logc->fhp);
		logc->fhp = NULL;
	}

	if (logc->dbt.data != NULL)
		__os_free(env, logc->dbt.data);

	__os_free(env, logc->bp);
	__os_free(env, logc);

	return (0);
}

/*
 * __logc_version_pp --
 *	DB_LOGC->version.
 */
static int
__logc_version_pp(logc, versionp, flags)
	DB_LOGC *logc;
	u_int32_t *versionp;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = logc->env;

	if ((ret = __db_fchk(env, "DB_LOGC->version", flags, 0)) != 0)
		return (ret);

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__logc_version(logc, versionp)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __logc_version --
 *	DB_LOGC->version.
 *
 * PUBLIC: int __logc_version __P((DB_LOGC *, u_int32_t *));
 */
int
__logc_version(logc, versionp)
	DB_LOGC *logc;
	u_int32_t *versionp;
{
	DBT hdrdbt;
	DB_LOGC *plogc;
	DB_LSN plsn;
	ENV *env;
	LOGP *persist;
	int ret, t_ret;

	env = logc->env;
	if (IS_ZERO_LSN(logc->lsn)) {
		__db_errx(env, DB_STR("2574", "DB_LOGC->get: unset cursor"));
		return (EINVAL);
	}
	ret = 0;
	/*
	 * Check if the persist info we have is for the same file
	 * as the current cursor position.  If we already have the
	 * information, then we're done.  If not, we open a new
	 * log cursor and get the header.
	 *
	 * Since most users walk forward through the log when
	 * using this feature (i.e. printlog) we're likely to
	 * have the information we need.
	 */
	if (logc->lsn.file != logc->p_lsn.file) {
		if ((ret = __log_cursor(env, &plogc)) != 0)
			return (ret);
		plsn.file = logc->lsn.file;
		plsn.offset = 0;
		plogc->lsn = plsn;
		memset(&hdrdbt, 0, sizeof(DBT));
		if ((ret = __logc_get_int(plogc,
		    &plsn, &hdrdbt, DB_SET)) == 0) {
			persist = (LOGP *)hdrdbt.data;
			if (LOG_SWAPPED(env))
				__log_persistswap(persist);
			logc->p_lsn = logc->lsn;
			logc->p_version = persist->version;
		}
		if ((t_ret = __logc_close(plogc)) != 0 && ret == 0)
			ret = t_ret;
	}
	/* Return the version. */
	if (ret == 0)
		*versionp = logc->p_version;
	return (ret);
}

/*
 * __logc_get_pp --
 *	DB_LOGC->get pre/post processing.
 */
static int
__logc_get_pp(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret;

	env = logc->env;

	/* Validate arguments. */
	switch (flags) {
	case DB_CURRENT:
	case DB_FIRST:
	case DB_LAST:
	case DB_NEXT:
	case DB_PREV:
		break;
	case DB_SET:
		if (IS_ZERO_LSN(*alsn)) {
			__db_errx(env, DB_STR_A("2575",
			    "DB_LOGC->get: invalid LSN: %lu/%lu", "%lu %lu"),
			    (u_long)alsn->file, (u_long)alsn->offset);
			return (EINVAL);
		}
		break;
	default:
		return (__db_ferr(env, "DB_LOGC->get", 1));
	}

	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env, (__logc_get(logc, alsn, dbt, flags)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __logc_get --
 *	DB_LOGC->get.
 *
 * PUBLIC: int __logc_get __P((DB_LOGC *, DB_LSN *, DBT *, u_int32_t));
 */
int
__logc_get(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_LSN saved_lsn;
	ENV *env;
	LOGP *persist;
	int ret;

	env = logc->env;

	/*
	 * On error, we take care not to overwrite the caller's LSN.  This
	 * is because callers looking for the end of the log loop using the
	 * DB_NEXT flag, and expect to take the last successful lsn out of
	 * the passed-in structure after DB_LOGC->get fails with DB_NOTFOUND.
	 *
	 * !!!
	 * This line is often flagged an uninitialized memory read during a
	 * Purify or similar tool run, as the application didn't initialize
	 * *alsn.  If the application isn't setting the DB_SET flag, there is
	 * no reason it should have initialized *alsn, but we can't know that
	 * and we want to make sure we never overwrite whatever the application
	 * put in there.
	 */
	saved_lsn = *alsn;
	/*
	 * If we get one of the log's header records as a result of doing a
	 * DB_FIRST, DB_NEXT, DB_LAST or DB_PREV, repeat the operation, log
	 * file header records aren't useful to applications.
	 */
	if ((ret = __logc_get_int(logc, alsn, dbt, flags)) != 0) {
		*alsn = saved_lsn;
		return (ret);
	}
	/*
	 * The DBT was populated by the call to __logc_get_int, copy the data
	 * out of DB_DBT_USERMEM space if it is there.
	 */
	if ((ret = __dbt_usercopy(env, dbt)) != 0)
		return (ret);

	if (alsn->offset == 0 && (flags == DB_FIRST ||
	    flags == DB_NEXT || flags == DB_LAST || flags == DB_PREV)) {
		switch (flags) {
		case DB_FIRST:
			flags = DB_NEXT;
			break;
		case DB_LAST:
			flags = DB_PREV;
			break;
		case DB_NEXT:
		case DB_PREV:
		default:
			break;
		}
		/*
		 * If we're walking the log and we find a persist header
		 * then store so that we may use it later if needed.
		 */
		persist = (LOGP *)dbt->data;
		if (LOG_SWAPPED(env))
			__log_persistswap(persist);
		logc->p_lsn = *alsn;
		logc->p_version = persist->version;
		if (F_ISSET(dbt, DB_DBT_MALLOC)) {
			__os_free(env, dbt->data);
			dbt->data = NULL;
		}
		if ((ret = __logc_get_int(logc, alsn, dbt, flags)) != 0) {
			*alsn = saved_lsn;
			goto err;
		}
	}

err:	__dbt_userfree(env, dbt, NULL, NULL);
	return (ret);
}

/*
 * __logc_get_int --
 *	Get a log record; internal version.
 */
static int
__logc_get_int(logc, alsn, dbt, flags)
	DB_LOGC *logc;
	DB_LSN *alsn;
	DBT *dbt;
	u_int32_t flags;
{
	DB_CIPHER *db_cipher;
	DB_LOG *dblp;
	DB_LSN last_lsn, nlsn;
	ENV *env;
	HDR hdr;
	LOG *lp;
	RLOCK rlock;
	logfile_validity status;
	u_int32_t cnt, logfsz, orig_flags;
	u_int8_t *rp;
	int eof, is_hmac, need_cksum, ret;
	size_t blen;
#ifdef HAVE_LOG_CHECKSUM
	u_int32_t i, logtype, version;
	char chksumbuf[256];
	u_int8_t ch;
#endif

	env = logc->env;
	db_cipher = env->crypto_handle;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	eof = is_hmac = 0;
	orig_flags = flags; /* flags may be altered later. */
	blen = 0;
	logfsz = lp->persist.log_size;

	/*
	 * We don't acquire the log region lock until we need it, and we
	 * release it as soon as we're done.
	 */
	rlock = F_ISSET(logc, DB_LOG_LOCKED) ? L_ALREADY : L_NONE;

#ifdef HAVE_LOG_CHECKSUM
nextrec:
#endif
	nlsn = logc->lsn;
	switch (flags) {
	case DB_NEXT:				/* Next log record. */
		if (!IS_ZERO_LSN(nlsn)) {
			/* Increment the cursor by the cursor record size. */
			nlsn.offset += logc->len;
			break;
		}
		flags = DB_FIRST;
		/* FALLTHROUGH */
	case DB_FIRST:				/* First log record. */
		/* Find the first log file. */
		if ((ret = __log_find(dblp, 1, &cnt, &status)) != 0)
			goto err;

		/*
		 * DB_LV_INCOMPLETE:
		 *	Theoretically, the log file we want could be created
		 *	but not yet written, the "first" log record must be
		 *	in the log buffer.
		 * DB_LV_NORMAL:
		 * DB_LV_OLD_READABLE:
		 *	We found a log file we can read.
		 * DB_LV_NONEXISTENT:
		 *	No log files exist, the "first" log record must be in
		 *	the log buffer.
		 * DB_LV_OLD_UNREADABLE:
		 *	No readable log files exist, we're at the cross-over
		 *	point between two versions.  The "first" log record
		 *	must be in the log buffer.
		 */
		switch (status) {
		case DB_LV_INCOMPLETE:
			DB_ASSERT(env, lp->lsn.file == cnt);
			/* FALLTHROUGH */
		case DB_LV_NORMAL:
		case DB_LV_OLD_READABLE:
			nlsn.file = cnt;
			break;
		case DB_LV_NONEXISTENT:
			nlsn.file = 1;
			DB_ASSERT(env, lp->lsn.file == nlsn.file);
			break;
		case DB_LV_OLD_UNREADABLE:
			nlsn.file = cnt + 1;
			DB_ASSERT(env, lp->lsn.file == nlsn.file);
			break;
		}
		nlsn.offset = 0;
		break;
	case DB_CURRENT:			/* Current log record. */
		break;
	case DB_PREV:				/* Previous log record. */
		if (!IS_ZERO_LSN(nlsn)) {
			/* If at start-of-file, move to the previous file. */
			if (nlsn.offset == 0) {
				if (nlsn.file == 1) {
					ret = DB_NOTFOUND;
					goto err;
				}
				if ((!lp->db_log_inmemory &&
				    (__log_valid(dblp, nlsn.file - 1, 0, NULL,
				    0, &status, NULL) != 0 ||
				    (status != DB_LV_NORMAL &&
				    status != DB_LV_OLD_READABLE)))) {
					ret = DB_NOTFOUND;
					goto err;
				}

				--nlsn.file;
			}
			nlsn.offset = logc->prev;
			break;
		}
		/* FALLTHROUGH */
	case DB_LAST:				/* Last log record. */
		if (rlock == L_NONE) {
			rlock = L_ACQUIRED;
			LOG_SYSTEM_LOCK(env);
		}
		nlsn.file = lp->lsn.file;
		nlsn.offset = lp->lsn.offset - lp->len;
		break;
	case DB_SET:				/* Set log record. */
		nlsn = *alsn;
		break;
	default:
		ret = __db_unknown_path(env, "__logc_get_int");
		goto err;
	}

	if (0) {				/* Move to the next file. */
next_file:	++nlsn.file;
		nlsn.offset = 0;
	}

	/*
	 * The above switch statement should have set nlsn to the lsn of
	 * the requested record.
	 */

	if (CRYPTO_ON(env)) {
		hdr.size = HDR_CRYPTO_SZ;
		is_hmac = 1;
	} else {
		hdr.size = HDR_NORMAL_SZ;
		is_hmac = 0;
	}

	/*
	 * Check to see if the record is in the cursor's buffer -- if so,
	 * we'll need to checksum it.
	 */
	if ((ret = __logc_incursor(logc, &nlsn, &hdr, &rp)) != 0)
		goto err;
	if (rp != NULL)
		goto cksum;

	/*
	 * Look to see if we're moving backward in the log with the last record
	 * coming from the disk -- it means the record can't be in the region's
	 * buffer.  Else, check the region's buffer.
	 *
	 * If the record isn't in the region's buffer, then either logs are
	 * in-memory, and we're done, or we're going to have to read the
	 * record from disk.  We want to make a point of not reading past the
	 * end of the logical log (after recovery, there may be data after the
	 * end of the logical log, not to mention the log file may have been
	 * pre-allocated).  So, zero out last_lsn, and initialize it inside
	 * __logc_inregion -- if it's still zero when we check it in
	 * __logc_ondisk, that's OK, it just means the logical end of the log
	 * isn't an issue for this request.
	 */
	ZERO_LSN(last_lsn);
	if (!F_ISSET(logc, DB_LOG_DISK) ||
	    LOG_COMPARE(&nlsn, &logc->lsn) > 0) {
		F_CLR(logc, DB_LOG_DISK);

		if ((ret = __logc_inregion(logc,
		    &nlsn, &rlock, &last_lsn, &hdr, &rp, &need_cksum)) != 0)
			goto err;
		if (rp != NULL) {
			/*
			 * If we read the entire record from the in-memory log
			 * buffer, we don't need to checksum it, nor do we need
			 * to worry about vtruncate issues.
			 */
			if (need_cksum)
				goto cksum;
			goto from_memory;
		}
		if (lp->db_log_inmemory)
			goto nohdr;
	}

	/*
	 * We have to read from an on-disk file to retrieve the record.
	 * If we ever can't retrieve the record at offset 0, we're done,
	 * return EOF/DB_NOTFOUND.
	 *
	 * Discard the region lock if we're still holding it, the on-disk
	 * reading routines don't need it.
	 */
	if (rlock == L_ACQUIRED) {
		rlock = L_NONE;
		LOG_SYSTEM_UNLOCK(env);
	}
	if ((ret = __logc_ondisk(
	    logc, &nlsn, &last_lsn, flags, &hdr, &rp, &eof)) != 0)
		goto err;

	/*
	 * If we got a 0-length record, that means we're in the midst of some
	 * bytes that got 0'd as the result of a vtruncate.  In that case or at
	 * the end of a file, with DB_NEXT we're going to have to retry.
	 */
	if (eof || hdr.len == 0) {
nohdr:		switch (flags) {
		case DB_LAST:
		case DB_PREV:
			/*
			 * We should never get here.  If we recover a log
			 * file with 0's at the end, we'll treat the 0'd
			 * headers as the end of log and ignore them.  If
			 * we're reading backwards from another file, then
			 * the first record in that new file should have its
			 * prev field set correctly.
			 * First check that the file exists.
			 */
			if (eof && logc->bp_lsn.file != nlsn.file)
				__db_errx(env, DB_STR_A("2583",
	     "Log file %d not found, check log directory configuration", "%d"),
	     			     nlsn.file);
			else
				__db_errx(env, DB_STR("2576",
		"Encountered zero length records while traversing backwards"));
			ret = __env_panic(env, DB_RUNRECOVERY);
			goto err;
		case DB_FIRST:
		case DB_NEXT:
			/*
			 * Zero'd records always indicate the end of a file,
			 * but only go to the next file once.
			 */
			if (nlsn.offset != 0)
				goto next_file;
			/* FALLTHROUGH */
		case DB_SET:
		default:
			ret = DB_NOTFOUND;
			goto err;
		}
	}

	F_SET(logc, DB_LOG_DISK);

cksum:	/*
	 * Discard the region lock if we're still holding it.  (The path to
	 * get here is we acquired the region lock because of the caller's
	 * flag argument, but we found the record in the in-memory or cursor
	 * buffers.  Improbable, but it's easy to avoid.)
	 */
	if (rlock == L_ACQUIRED) {
		rlock = L_NONE;
		LOG_SYSTEM_UNLOCK(env);
	}
#ifdef HAVE_LOG_CHECKSUM
	/*
	 * Checksum: there are two types of errors -- a configuration error
	 * or a checksum mismatch.  The former is always bad.  The latter is
	 * OK if we're searching for the end of the log, and very, very bad
	 * if we're reading random log records.
	 */
	if ((ret = __db_check_chksum(env, &hdr, db_cipher,
	    hdr.chksum, rp + hdr.size, hdr.len - hdr.size, is_hmac)) != 0) {
		/*
		 * This might be a log whose checksum does not include the hdr.
		 * Try again without the header, either for logs whose version
		 * is pre-DB_LOGCHKSUM, or for the persist record which contains
		 * the log version. Check for the zero offset first to avoid
		 * unwanted recursion in __logc_version().
		 *
		 * Set the cursor to the LSN we are trying to look at.
		 */
		last_lsn = logc->lsn;
		logc->lsn = nlsn;
		if ((logc->lsn.offset == 0 ||
		    (__logc_version(logc, &version) == 0 &&
		    version < DB_LOGCHKSUM)) &&
		    __db_check_chksum(env, NULL,  db_cipher, hdr.chksum,
		    rp + hdr.size, hdr.len - hdr.size, is_hmac) == 0) {
			logc->lsn = last_lsn;
			goto from_memory;
		}

		/*
		 * If we are iterating logs during log verification and basic
		 * header info is correct, we can skip the failed log record
		 * and goto next one.
		 */
		if (F_ISSET(logc->env->lg_handle, DBLOG_VERIFYING) &&
		    (orig_flags == DB_FIRST || orig_flags == DB_LAST ||
		    orig_flags == DB_PREV || orig_flags == DB_NEXT) &&
		    hdr.size > 0 && hdr.len > hdr.size && hdr.len < logfsz &&
		    (((flags == DB_FIRST || flags == DB_NEXT) &&
		    hdr.prev == last_lsn.offset) ||
		    ((flags == DB_PREV || flags == DB_LAST) &&
		    last_lsn.offset - hdr.len == nlsn.offset))) {

			flags = orig_flags;

			logc->lsn = nlsn;
			logc->len = hdr.len;
			logc->prev = hdr.prev;

			if (flags == DB_LAST)
				flags = DB_PREV;
			else if (flags == DB_FIRST)
				flags = DB_NEXT;

			memset(chksumbuf, 0, 256);
			blen = 0;
			for (i = 0; i < DB_MAC_KEY && blen < 256; i++) {
				ch = hdr.chksum[i];
				blen = strlen(chksumbuf);
				snprintf(chksumbuf + blen, 255 - blen,
				    isprint(ch) ||
				    ch == 0x0a ? "%c" : "%#x ", ch);
			}
			/* Type field is always the first one in the record. */
			memcpy(&logtype, rp + hdr.size, sizeof(logtype));
			__db_errx(env, DB_STR_A("2577",
			    "DB_LOGC->get: log record LSN %lu/%lu: "
			    "checksum mismatch, hdr.chksum: %s, hdr.prev: %u, "
			    "hdr.len: %u, log type: %u. Skipping it and "
			    "continuing with the %s one",
			    "%lu %lu %s %u %u %u %s"),
			    (u_long)nlsn.file, (u_long)nlsn.offset, chksumbuf,
			    hdr.prev, hdr.len, logtype, flags == DB_NEXT ?
			    DB_STR_P("next") : DB_STR_P("previous"));
			goto nextrec;
		}

		if (F_ISSET(logc, DB_LOG_SILENT_ERR)) {
			if (ret == -1)
				ret = EIO;
		} else if (ret == -1) {
			__db_errx(env, DB_STR_A("2578",
		    "DB_LOGC->get: log record LSN %lu/%lu: checksum mismatch",
			    "%lu %lu"), (u_long)nlsn.file, (u_long)nlsn.offset);
			__db_errx(env, DB_STR("2579",
		    "DB_LOGC->get: catastrophic recovery may be required"));
			ret = __env_panic(env, DB_RUNRECOVERY);
		}
		logc->lsn = last_lsn;
		goto err;
	}
#endif

from_memory:
	/*
	 * Discard the region lock if we're still holding it.  (The path to
	 * get here is we acquired the region lock because of the caller's
	 * flag argument, but we found the record in the in-memory or cursor
	 * buffers.  Improbable, but it's easy to avoid.)
	 */
	if (rlock == L_ACQUIRED) {
		rlock = L_NONE;
		LOG_SYSTEM_UNLOCK(env);
	}

	/* Copy the record into the user's DBT. */
	if ((ret = __db_retcopy(env, dbt, rp + hdr.size,
	    (u_int32_t)(hdr.len - hdr.size),
	    &logc->dbt.data, &logc->dbt.ulen)) != 0)
		goto err;

	if (CRYPTO_ON(env)) {
		if ((ret = db_cipher->decrypt(env, db_cipher->data,
		    hdr.iv, dbt->data, hdr.len - hdr.size)) != 0) {
			ret = EAGAIN;
			goto err;
		}
		/*
		 * Return the original log record size to the user,
		 * even though we've allocated more than that, possibly.
		 * The log record is decrypted in the user dbt, not in
		 * the buffer, so we must do this here after decryption,
		 * not adjust the len passed to the __db_retcopy call.
		 */
		dbt->size = hdr.orig_size;
	}

	/* Update the cursor and the returned LSN. */
	*alsn = nlsn;
	logc->lsn = nlsn;
	logc->len = hdr.len;
	logc->prev = hdr.prev;

err:	if (rlock == L_ACQUIRED)
		LOG_SYSTEM_UNLOCK(env);

	return (ret);
}

/*
 * __logc_incursor --
 *	Check to see if the requested record is in the cursor's buffer.
 */
static int
__logc_incursor(logc, lsn, hdr, pp)
	DB_LOGC *logc;
	DB_LSN *lsn;
	HDR *hdr;
	u_int8_t **pp;
{
	ENV *env;
	u_int8_t *p;
	int eof;

	env = logc->env;
	*pp = NULL;

	/*
	 * Test to see if the requested LSN could be part of the cursor's
	 * buffer.
	 *
	 * The record must be part of the same file as the cursor's buffer.
	 * The record must start at a byte offset equal to or greater than
	 * the cursor buffer.
	 * The record must not start at a byte offset after the cursor
	 * buffer's end.
	 */
	if (logc->bp_lsn.file != lsn->file)
		return (0);
	if (logc->bp_lsn.offset > lsn->offset)
		return (0);
	if (logc->bp_lsn.offset + logc->bp_rlen <= lsn->offset + hdr->size)
		return (0);

	/*
	 * Read the record's header and check if the record is entirely held
	 * in the buffer.  If the record is not entirely held, get it again.
	 * (The only advantage in having part of the record locally is that
	 * we might avoid a system call because we already have the HDR in
	 * memory.)
	 *
	 * If the header check fails for any reason, it must be because the
	 * LSN is bogus.  Fail hard.
	 */
	p = logc->bp + (lsn->offset - logc->bp_lsn.offset);
	memcpy(hdr, p, hdr->size);
	if (LOG_SWAPPED(env))
		__log_hdrswap(hdr, CRYPTO_ON(env));
	if (__logc_hdrchk(logc, lsn, hdr, &eof))
		return (DB_NOTFOUND);
	if (eof || logc->bp_lsn.offset + logc->bp_rlen < lsn->offset + hdr->len)
		return (0);

	*pp = p;				/* Success. */

	return (0);
}

/*
 * __logc_inregion --
 *	Check to see if the requested record is in the region's buffer.
 */
static int
__logc_inregion(logc, lsn, rlockp, last_lsn, hdr, pp, need_cksump)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	RLOCK *rlockp;
	HDR *hdr;
	u_int8_t **pp;
	int *need_cksump;
{
	DB_LOG *dblp;
	ENV *env;
	LOG *lp;
	size_t b_region, len, nr;
	u_int32_t b_disk;
	int eof, ret;
	u_int8_t *p;

	env = logc->env;
	dblp = env->lg_handle;
	lp = env->lg_handle->reginfo.primary;

	ret = 0;
	b_region = 0;
	*pp = NULL;
	*need_cksump = 0;

	/* If we haven't yet acquired the log region lock, do so. */
	if (*rlockp == L_NONE) {
		*rlockp = L_ACQUIRED;
		LOG_SYSTEM_LOCK(env);
	}

	/*
	 * The routines to read from disk must avoid reading past the logical
	 * end of the log, so pass that information back to it.
	 *
	 * Since they're reading directly from the disk, they must also avoid
	 * reading past the offset we've written out.  If the log was
	 * truncated, it's possible that there are zeroes or garbage on
	 * disk after this offset, and the logical end of the log can
	 * come later than this point if the log buffer isn't empty.
	 */
	*last_lsn = lp->lsn;
	if (!lp->db_log_inmemory && last_lsn->offset > lp->w_off)
		last_lsn->offset = lp->w_off;

	/*
	 * Test to see if the requested LSN could be part of the region's
	 * buffer.
	 *
	 * During recovery, we read the log files getting the information to
	 * initialize the region.  In that case, the region's lsn field will
	 * not yet have been filled in, use only the disk.
	 *
	 * The record must not start at a byte offset after the region buffer's
	 * end, since that means the request is for a record after the end of
	 * the log.  Do this test even if the region's buffer is empty -- after
	 * recovery, the log files may continue past the declared end-of-log,
	 * and the disk reading routine will incorrectly attempt to read the
	 * remainder of the log.
	 *
	 * Otherwise, test to see if the region's buffer actually has what we
	 * want:
	 *
	 * The buffer must have some useful content.
	 * The record must be in the same file as the region's buffer and must
	 * start at a byte offset equal to or greater than the region's buffer.
	 */
	if (IS_ZERO_LSN(lp->lsn))
		return (0);
	if (LOG_COMPARE(lsn, &lp->lsn) >= 0)
		return (DB_NOTFOUND);
	else if (lp->db_log_inmemory) {
		if ((ret = __log_inmem_lsnoff(dblp, lsn, &b_region)) != 0)
			return (ret);
	} else if (lp->b_off == 0 || LOG_COMPARE(lsn, &lp->f_lsn) < 0)
		return (0);

	/*
	 * The current contents of the cursor's buffer will be useless for a
	 * future call, we're about to overwrite it -- trash it rather than
	 * try and make it look correct.
	 */
	logc->bp_rlen = 0;

	/*
	 * If the requested LSN is greater than the region buffer's first
	 * byte, we know the entire record is in the buffer on a good LSN.
	 *
	 * If we're given a bad LSN, the "entire" record might not be in
	 * our buffer in order to fail at the chksum.  __logc_hdrchk made
	 * sure our dest buffer fits, via bp_maxrec, but we also need to
	 * make sure we don't run off the end of this buffer, the src.
	 *
	 * There is one case where the header check can fail: on a scan through
	 * in-memory logs, when we reach the end of a file we can read an empty
	 * header.  In that case, it's safe to return zero, here: it will be
	 * caught in our caller.  Otherwise, the LSN is bogus.  Fail hard.
	 */
	if (lp->db_log_inmemory || LOG_COMPARE(lsn, &lp->f_lsn) > 0) {
		if (!lp->db_log_inmemory)
			b_region = lsn->offset - lp->w_off;
		__log_inmem_copyout(dblp, b_region, hdr, hdr->size);
		if (LOG_SWAPPED(env))
			__log_hdrswap(hdr, CRYPTO_ON(env));
		if (__logc_hdrchk(logc, lsn, hdr, &eof) != 0)
			return (DB_NOTFOUND);
		if (eof)
			return (0);
		if (lp->db_log_inmemory) {
			if (RINGBUF_LEN(lp, b_region, lp->b_off) < hdr->len)
				return (DB_NOTFOUND);
		} else if (lsn->offset + hdr->len > lp->w_off + lp->buffer_size)
			return (DB_NOTFOUND);
		if (logc->bp_size <= hdr->len) {
			len = (size_t)DB_ALIGN((uintmax_t)hdr->len * 2, 128);
			if ((ret =
			    __os_realloc(logc->env, len, &logc->bp)) != 0)
				 return (ret);
			logc->bp_size = (u_int32_t)len;
		}
		__log_inmem_copyout(dblp, b_region, logc->bp, hdr->len);
		*pp = logc->bp;
		return (0);
	}

	DB_ASSERT(env, !lp->db_log_inmemory);

	/*
	 * There's a partial record, that is, the requested record starts
	 * in a log file and finishes in the region buffer.  We have to
	 * find out how many bytes of the record are in the region buffer
	 * so we can copy them out into the cursor buffer.  First, check
	 * to see if the requested record is the only record in the region
	 * buffer, in which case we should copy the entire region buffer.
	 *
	 * Else, walk back through the region's buffer to find the first LSN
	 * after the record that crosses the buffer boundary -- we can detect
	 * that LSN, because its "prev" field will reference the record we
	 * want.  The bytes we need to copy from the region buffer are the
	 * bytes up to the record we find.  The bytes we'll need to allocate
	 * to hold the log record are the bytes between the two offsets.
	 */
	b_disk = lp->w_off - lsn->offset;
	if (lp->b_off <= lp->len)
		b_region = (u_int32_t)lp->b_off;
	else
		for (p = dblp->bufp + (lp->b_off - lp->len);;) {
			memcpy(hdr, p, hdr->size);
			if (LOG_SWAPPED(env))
				__log_hdrswap(hdr, CRYPTO_ON(env));
			if (hdr->prev == lsn->offset) {
				b_region = (u_int32_t)(p - dblp->bufp);
				break;
			}
			p = dblp->bufp + (hdr->prev - lp->w_off);
		}

	/*
	 * If we don't have enough room for the record, we have to allocate
	 * space.  We have to do it while holding the region lock, which is
	 * truly annoying, but there's no way around it.  This call is why
	 * we allocate cursor buffer space when allocating the cursor instead
	 * of waiting.
	 */
	if (logc->bp_size <= b_region + b_disk) {
		len = (size_t)DB_ALIGN((uintmax_t)(b_region + b_disk) * 2, 128);
		if ((ret = __os_realloc(logc->env, len, &logc->bp)) != 0)
			return (ret);
		logc->bp_size = (u_int32_t)len;
	}

	/* Copy the region's bytes to the end of the cursor's buffer. */
	p = (logc->bp + logc->bp_size) - b_region;
	memcpy(p, dblp->bufp, b_region);

	/* Release the region lock. */
	if (*rlockp == L_ACQUIRED) {
		*rlockp = L_NONE;
		LOG_SYSTEM_UNLOCK(env);
	}

	/*
	 * Read the rest of the information from disk.  Neither short reads
	 * or EOF are acceptable, the bytes we want had better be there.
	 */
	if (b_disk != 0) {
		p -= b_disk;
		nr = b_disk;
		if ((ret = __logc_io(
		    logc, lsn->file, lsn->offset, p, &nr, NULL)) != 0)
			return (ret);
		if (nr < b_disk)
			return (__logc_shortread(logc, lsn, 0));

		/* We read bytes from the disk, we'll need to checksum them. */
		*need_cksump = 1;
	}

	/* Copy the header information into the caller's structure. */
	memcpy(hdr, p, hdr->size);
	if (LOG_SWAPPED(env))
		__log_hdrswap(hdr, CRYPTO_ON(env));

	*pp = p;
	return (0);
}

/*
 * __log_hdrswap --
 *	Swap the bytes in a log header from machines with different endianness.
 *
 * PUBLIC: void __log_hdrswap __P((HDR *, int));
 */
void
__log_hdrswap(hdr, is_hmac)
	HDR *hdr;
	int is_hmac;
{
	M_32_SWAP(hdr->prev);
	M_32_SWAP(hdr->len);
	if (!is_hmac)
		P_32_SWAP(hdr->chksum);
}

/*
 * __log_persistswap --
 *	Swap the bytes in a log file persistent header from machines with
 *	different endianness.
 *
 * PUBLIC: void __log_persistswap __P((LOGP *));
 */
void
__log_persistswap(persist)
	LOGP *persist;
{
	M_32_SWAP(persist->magic);
	M_32_SWAP(persist->version);
	M_32_SWAP(persist->log_size);
	M_32_SWAP(persist->notused);
}

/*
 * __logc_ondisk --
 *	Read a record off disk.
 */
static int
__logc_ondisk(logc, lsn, last_lsn, flags, hdr, pp, eofp)
	DB_LOGC *logc;
	DB_LSN *lsn, *last_lsn;
	u_int32_t flags;
	int *eofp;
	HDR *hdr;
	u_int8_t **pp;
{
	ENV *env;
	size_t len, nr;
	u_int32_t offset;
	int ret;

	env = logc->env;
	*eofp = 0;

	nr = hdr->size;
	if ((ret =
	    __logc_io(logc, lsn->file, lsn->offset, hdr, &nr, eofp)) != 0)
		return (ret);
	if (*eofp)
		return (0);

	if (LOG_SWAPPED(env))
		__log_hdrswap(hdr, CRYPTO_ON(env));

	/*
	 * If the read was successful, but we can't read a full header, assume
	 * we've hit EOF.  We can't check that the header has been partially
	 * zeroed out, but it's unlikely that this is caused by a write failure
	 * since the header is written as a single write call and it's less
	 * than sector.
	 */
	if (nr < hdr->size) {
		*eofp = 1;
		return (0);
	}

	/* Check the HDR. */
	if ((ret = __logc_hdrchk(logc, lsn, hdr, eofp)) != 0)
		return (ret);
	if (*eofp)
		return (0);

	/*
	 * Regardless of how we return, the previous contents of the cursor's
	 * buffer are useless -- trash it.
	 */
	logc->bp_rlen = 0;

	/*
	 * Otherwise, we now (finally!) know how big the record is.  (Maybe
	 * we should have just stuck the length of the record into the LSN!?)
	 * Make sure we have enough space.
	 */
	if (logc->bp_size <= hdr->len) {
		len = (size_t)DB_ALIGN((uintmax_t)hdr->len * 2, 128);
		if ((ret = __os_realloc(env, len, &logc->bp)) != 0)
			return (ret);
		logc->bp_size = (u_int32_t)len;
	}

	/*
	 * If we're moving forward in the log file, read this record in at the
	 * beginning of the buffer.  Otherwise, read this record in at the end
	 * of the buffer, making sure we don't try and read before the start
	 * of the file.  (We prefer positioning at the end because transaction
	 * aborts use DB_SET to move backward through the log and we might get
	 * lucky.)
	 *
	 * Read a buffer's worth, without reading past the logical EOF.  The
	 * last_lsn may be a zero LSN, but that's OK, the test works anyway.
	 */
	if (flags == DB_FIRST || flags == DB_NEXT)
		offset = lsn->offset;
	else if (lsn->offset + hdr->len < logc->bp_size)
		offset = 0;
	else
		offset = (lsn->offset + hdr->len) - logc->bp_size;

	nr = logc->bp_size;
	if (lsn->file == last_lsn->file && offset + nr >= last_lsn->offset)
		nr = last_lsn->offset - offset;

	if ((ret =
	    __logc_io(logc, lsn->file, offset, logc->bp, &nr, eofp)) != 0)
		return (ret);

	/*
	 * We should have at least gotten the bytes up-to-and-including the
	 * record we're reading.
	 */
	if (nr < (lsn->offset + hdr->len) - offset)
		return (__logc_shortread(logc, lsn, 1));

	/*
	 * Set up the return information.
	 *
	 * !!!
	 * No need to set the bp_lsn.file field, __logc_io set it for us.
	 */
	logc->bp_rlen = (u_int32_t)nr;
	logc->bp_lsn.offset = offset;

	*pp = logc->bp + (lsn->offset - offset);

	return (0);
}

/*
 * __logc_hdrchk --
 *
 * Check for corrupted HDRs before we use them to allocate memory or find
 * records.
 *
 * If the log files were pre-allocated, a zero-filled HDR structure is the
 * logical file end.  However, we can see buffers filled with 0's during
 * recovery, too (because multiple log buffers were written asynchronously,
 * and one made it to disk before a different one that logically precedes
 * it in the log file.
 *
 * Check for impossibly large records.  The malloc should fail later, but we
 * have customers that run mallocs that treat all allocation failures as fatal
 * errors.
 *
 * Note that none of this is necessarily something awful happening.  We let
 * the application hand us any LSN they want, and it could be a pointer into
 * the middle of a log record, there's no way to tell.
 */
static int
__logc_hdrchk(logc, lsn, hdr, eofp)
	DB_LOGC *logc;
	DB_LSN *lsn;
	HDR *hdr;
	int *eofp;
{
	ENV *env;
	int ret;

	env = logc->env;

	/*
	 * Check EOF before we do any other processing.
	 */
	if (eofp != NULL) {
		if (hdr->prev == 0 && hdr->chksum[0] == 0 && hdr->len == 0) {
			*eofp = 1;
			return (0);
		}
		*eofp = 0;
	}

	/*
	 * Sanity check the log record's size.
	 * We must check it after "virtual" EOF above.
	 */
	if (hdr->len <= hdr->size)
		goto err;

	/*
	 * If the cursor's max-record value isn't yet set, it means we aren't
	 * reading these records from a log file and no check is necessary.
	 */
	if (logc->bp_maxrec != 0 && hdr->len > logc->bp_maxrec) {
		/*
		 * If we fail the check, there's the pathological case that
		 * we're reading the last file, it's growing, and our initial
		 * check information was wrong.  Get it again, to be sure.
		 */
		if ((ret = __logc_set_maxrec(logc, NULL)) != 0) {
			__db_err(env, ret, "DB_LOGC->get");
			return (ret);
		}
		if (logc->bp_maxrec != 0 && hdr->len > logc->bp_maxrec)
			goto err;
	}
	return (0);

err:	if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
		__db_errx(env, DB_STR_A("2580",
		    "DB_LOGC->get: LSN %lu/%lu: invalid log record header",
		    "%lu %lu"), (u_long)lsn->file, (u_long)lsn->offset);
	return (EIO);
}

/*
 * __logc_io --
 *	Read records from a log file.
 */
static int
__logc_io(logc, fnum, offset, p, nrp, eofp)
	DB_LOGC *logc;
	u_int32_t fnum, offset;
	void *p;
	size_t *nrp;
	int *eofp;
{
	DB_LOG *dblp;
	ENV *env;
	LOG *lp;
	int ret;
	char *np;

	env = logc->env;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	/*
	 * If we've switched files, discard the current file handle and acquire
	 * a new one.
	 */
	if (logc->fhp != NULL && logc->bp_lsn.file != fnum) {
		ret = __os_closehandle(env, logc->fhp);
		logc->fhp = NULL;
		logc->bp_lsn.file = 0;

		if (ret != 0)
			return (ret);
	}
	if (logc->fhp == NULL) {
		if ((ret = __log_name(dblp, fnum,
		    &np, &logc->fhp, DB_OSO_RDONLY | DB_OSO_SEQ)) != 0) {
			/*
			 * If we're allowed to return EOF, assume that's the
			 * problem, set the EOF status flag and return 0.
			 */
			if (eofp != NULL) {
				*eofp = 1;
				ret = 0;
			} else if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
				__db_err(env, ret, "DB_LOGC->get: %s",
				    np == NULL ? "__log_name failed" : np);
			__os_free(env, np);
			return (ret);
		}

		if ((ret = __logc_set_maxrec(logc, np)) != 0) {
			__db_err(env, ret, "DB_LOGC->get: %s", np);
			__os_free(env, np);
			return (ret);
		}
		__os_free(env, np);

		logc->bp_lsn.file = fnum;
	}

	STAT_INC(env, log, read, lp->stat.st_rcount, fnum);
	/* Seek to the record's offset and read the data. */
	if ((ret = __os_io(env, DB_IO_READ,
	    logc->fhp, 0, 0, offset, (u_int32_t)*nrp, p, nrp)) != 0) {
		if (!F_ISSET(logc, DB_LOG_SILENT_ERR))
			__db_err(env, ret, DB_STR_A("2581",
			    "DB_LOGC->get: LSN: %lu/%lu: read", "%lu %lu"),
			    (u_long)fnum, (u_long)offset);
		return (ret);
	}

	return (0);
}

/*
 * __logc_shortread --
 *	Read was short -- return a consistent error message and error.
 */
static int
__logc_shortread(logc, lsn, check_silent)
	DB_LOGC *logc;
	DB_LSN *lsn;
	int check_silent;
{
	if (!check_silent || !F_ISSET(logc, DB_LOG_SILENT_ERR))
		__db_errx(logc->env, DB_STR_A("2582",
		    "DB_LOGC->get: LSN: %lu/%lu: short read", "%lu %lu"),
		    (u_long)lsn->file, (u_long)lsn->offset);
	return (EIO);
}

/*
 * __logc_set_maxrec --
 *	Bound the maximum log record size in a log file.
 */
static int
__logc_set_maxrec(logc, np)
	DB_LOGC *logc;
	char *np;
{
	DB_LOG *dblp;
	ENV *env;
	LOG *lp;
	u_int32_t mbytes, bytes;
	int ret;

	env = logc->env;
	dblp = env->lg_handle;

	/*
	 * We don't want to try and allocate huge chunks of memory because
	 * applications with error-checking malloc's often consider that a
	 * hard failure.  If we're about to look at a corrupted record with
	 * a bizarre size, we need to know before trying to allocate space
	 * to hold it.  We could read the persistent data at the beginning
	 * of the file but that's hard -- we may have to decrypt it, checksum
	 * it and so on.  Stat the file instead.
	 */
	if (logc->fhp != NULL) {
		if ((ret = __os_ioinfo(env, np, logc->fhp,
		    &mbytes, &bytes, NULL)) != 0)
			return (ret);
		if (logc->bp_maxrec < (mbytes * MEGABYTE + bytes))
			logc->bp_maxrec = mbytes * MEGABYTE + bytes;
	}

	/*
	 * If reading from the log file currently being written, we could get
	 * an incorrect size, that is, if the cursor was opened on the file
	 * when it had only a few hundred bytes, and then the cursor used to
	 * move forward in the file, after more log records were written, the
	 * original stat value would be wrong.  Use the maximum of the current
	 * log file size and the size of the buffer -- that should represent
	 * the max of any log record currently in the file.
	 *
	 * The log buffer size is set when the environment is opened and never
	 * changed, we don't need a lock on it.
	 */
	lp = dblp->reginfo.primary;
	if (logc->bp_maxrec < lp->buffer_size)
		logc->bp_maxrec = lp->buffer_size;

	return (0);
}

/*
 * PUBLIC: int __log_read_record_pp  __P((DB_ENV *, DB **, void *, void *,
 * PUBLIC:     DB_LOG_RECSPEC *, u_int32_t, void **));
 */
int
__log_read_record_pp(dbenv, dbpp, td, recbuf, spec, size, argpp)
	DB_ENV *dbenv;
	DB **dbpp;
	void *td;
	void *recbuf;
	DB_LOG_RECSPEC *spec;
	u_int32_t size;
	void **argpp;
{
	DB_THREAD_INFO *ip;
	int ret;

	ENV_REQUIRES_CONFIG(dbenv->env,
	    dbenv->env->lg_handle, "DB_ENV->log_read_record", DB_INIT_LOG);

	*argpp = NULL;
	ENV_ENTER(dbenv->env, ip);
	if ((ret = __os_umalloc(dbenv->env, size + sizeof(DB_TXN), argpp)) != 0)
		goto done;
	REPLICATION_WRAP(dbenv->env, (__log_read_record(dbenv->env, dbpp,
	    td, recbuf, spec, size, argpp)), 0, ret);
	if (ret != 0) {
		__os_ufree(dbenv->env, *argpp);
		*argpp = NULL;
	}
done:	ENV_LEAVE(dbenv->env, ip);
	return (ret);
}

/*
 * PUBLIC: int __log_read_record  __P((ENV *, DB **, void *, void *,
 * PUBLIC:     DB_LOG_RECSPEC *, u_int32_t, void **));
 */
int
__log_read_record(env, dbpp, td, recbuf, spec, size, argpp)
	ENV *env;
	DB **dbpp;
	void *td;
	void *recbuf;
	DB_LOG_RECSPEC *spec;
	u_int32_t size;
	void **argpp;
{
	DB_LOG_RECSPEC *sp, *np;
	DB_TXN *txnp;
	LOG *lp;
	PAGE *hdrstart;
	u_int32_t hdrsize, op, uinttmp;
	u_int8_t *ap, *bp;
	int has_data, ret, downrev;

	COMPQUIET(has_data, 0);
	COMPQUIET(hdrsize, 0);
	COMPQUIET(hdrstart, NULL);
	COMPQUIET(op, 0);
	ap = *argpp;
	/*
	 * Allocate space for the arg structure and a transaction
	 * structure which will imediately follow it.
	 */
	if (ap == NULL &&
	    (ret = __os_malloc(env, size + sizeof(DB_TXN), &ap)) != 0)
		return (ret);
	txnp = (DB_TXN *)(ap + size);
	memset(txnp, 0, sizeof(DB_TXN));
	txnp->td = td;
	lp = env->lg_handle->reginfo.primary;
	downrev = lp->persist.version < DB_LOGVERSION_50;

	bp = recbuf;

	/*
	 * The first three fields are always the same in every arg
	 * struct so we know their offsets.
	 */
	/* type */
	LOGCOPY_32(env, ap + SSZ(LOG_REC_HEADER, type), bp);
	bp += sizeof(u_int32_t);

	/* txnp */
	LOGCOPY_32(env, &txnp->txnid, bp);
	*(DB_TXN **)(ap + SSZ(LOG_REC_HEADER, txnp)) = txnp;
	bp += sizeof(txnp->txnid);

	/* Previous LSN */
	LOGCOPY_TOLSN(env,
	     (DB_LSN *)(ap +  SSZ(LOG_REC_HEADER, prev_lsn)), bp);
	bp += sizeof(DB_LSN);

	ret = 0;
	for (sp = spec; sp->type != LOGREC_Done; sp++) {
		switch (sp->type) {
		case LOGREC_DB:
			LOGCOPY_32(env, &uinttmp, bp);
			*(u_int32_t*)(ap + sp->offset) = uinttmp;
			bp += sizeof(uinttmp);
			if (dbpp != NULL) {
				*dbpp = NULL;
				ret = __dbreg_id_to_db(env,
				    txnp, dbpp, (int32_t)uinttmp, 1);
			}
			break;

		case LOGREC_ARG:
		case LOGREC_TIME:
		case LOGREC_DBOP:
			LOGCOPY_32(env, ap + sp->offset, bp);
			bp += sizeof(uinttmp);
			break;
		case LOGREC_OP:
			LOGCOPY_32(env, &op, bp);
			*(u_int32_t *)(ap + sp->offset) = op;
			bp += sizeof(uinttmp);
			break;
		case LOGREC_DBT:
		case LOGREC_PGLIST:
		case LOGREC_LOCKS:
		case LOGREC_HDR:
		case LOGREC_DATA:
		case LOGREC_PGDBT:
		case LOGREC_PGDDBT:
			memset(ap + sp->offset, 0, sizeof(DBT));
			LOGCOPY_32(env, &uinttmp, bp);
			*(u_int32_t*)
			     (ap + sp->offset + SSZ(DBT, size)) = uinttmp;
			bp += sizeof(u_int32_t);
			*(void **)(ap + sp->offset + SSZ(DBT, data)) = bp;

			/* Process fields that need to be byte swapped. */
			switch (sp->type) {
			case LOGREC_DBT:
			case LOGREC_PGLIST:
			case LOGREC_LOCKS:
				break;
			case LOGREC_HDR:
				if (uinttmp == 0)
					break;
				has_data = 0;
				for (np = sp + 1; np->type != LOGREC_Done; np++)
					if (np->type == LOGREC_DATA) {
						has_data = 1;
						break;
					}
				hdrstart = (PAGE *)bp;
				hdrsize = uinttmp;
				if (has_data == 1)
					break;
				/* FALLTHROUGH */
			case LOGREC_DATA:
				if (downrev ? LOG_SWAPPED(env) :
				    (dbpp != NULL && *dbpp != NULL &&
				    F_ISSET(*dbpp, DB_AM_SWAP)))
					__db_recordswap(op, hdrsize,
					    hdrstart, has_data ?
					    ap + sp->offset : NULL, 1);
				break;
			case LOGREC_PGDBT:
				has_data = 0;
				for (np = sp + 1; np->type != LOGREC_Done; np++)
					if (np->type == LOGREC_PGDDBT) {
						has_data = 1;
						break;
					}

				hdrstart = (PAGE *)bp;
				hdrsize = uinttmp;
				if (has_data == 1)
					break;
				/* FALLTHROUGH */
			case LOGREC_PGDDBT:
				if (dbpp != NULL && *dbpp != NULL &&
				    (downrev ? LOG_SWAPPED(env) :
				    F_ISSET(*dbpp, DB_AM_SWAP)) &&
				    (ret = __db_pageswap(env, *dbpp, hdrstart,
				    hdrsize, has_data == 0 ? NULL :
				    (DBT *)(ap + sp->offset), 1)) != 0)
					return (ret);
				break;
			default:
				DB_ASSERT(env, sp->type != sp->type);
			}

			bp += uinttmp;
			break;

		case LOGREC_POINTER:
			LOGCOPY_TOLSN(env, (DB_LSN *)(ap + sp->offset), bp);
			bp += sizeof(DB_LSN);
			break;

		default:
			DB_ASSERT(env, sp->type != sp->type);
		}
	}

	*argpp = ap;
	return (ret);
}
