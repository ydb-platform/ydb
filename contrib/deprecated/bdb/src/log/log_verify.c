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
#include "dbinc/btree.h"
#include "dbinc/fop.h"
#include "dbinc/hash.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#include "dbinc/log_verify.h"

#define	FIRST_OFFSET(env) \
	(sizeof(LOGP) + (CRYPTO_ON(env) ? HDR_CRYPTO_SZ : HDR_NORMAL_SZ))

static int __env_init_verify __P((ENV *, u_int32_t, DB_DISTAB *));

/*
 * PUBLIC: int __log_verify_pp __P((DB_ENV *, const DB_LOG_VERIFY_CONFIG *));
 */
int
__log_verify_pp(dbenv, lvconfig)
	DB_ENV *dbenv;
	const DB_LOG_VERIFY_CONFIG *lvconfig;
{
	int lsnrg, ret, timerg;
	DB_THREAD_INFO *ip;
	const char *phome;

	lsnrg = ret = timerg = 0;
	phome = NULL;

	if (!IS_ZERO_LSN(lvconfig->start_lsn) ||
	    !IS_ZERO_LSN(lvconfig->end_lsn))
		lsnrg = 1;
	if (lvconfig->start_time != 0 || lvconfig->end_time != 0)
		timerg = 1;

	if ((!IS_ZERO_LSN(lvconfig->start_lsn) && lvconfig->start_time != 0) ||
	    (!IS_ZERO_LSN(lvconfig->end_lsn) && lvconfig->end_time != 0) ||
	    (lsnrg && timerg)) {
		__db_errx(dbenv->env, DB_STR("2501",
		    "Set either an lsn range or a time range to verify logs "
		    "in the range, don't mix time and lsn."));
		ret = EINVAL;
		goto err;
	}
	phome = dbenv->env->db_home;
	if (phome != NULL && lvconfig->temp_envhome != NULL &&
	    strcmp(phome, lvconfig->temp_envhome) == 0) {
		__db_errx(dbenv->env,
		    "Environment home for log verification internal use "
		    "overlaps with that of the environment to verify.");
		ret = EINVAL;
		goto err;
	}

	ENV_ENTER(dbenv->env, ip);
	ret = __log_verify(dbenv, lvconfig, ip);
	ENV_LEAVE(dbenv->env, ip);
err:	return (ret);
}

/*
 * PUBLIC: int __log_verify __P((DB_ENV *, const DB_LOG_VERIFY_CONFIG *,
 * PUBLIC:     DB_THREAD_INFO *));
 */
int
__log_verify(dbenv, lvconfig, ip)
	DB_ENV *dbenv;
	const DB_LOG_VERIFY_CONFIG *lvconfig;
	DB_THREAD_INFO *ip;
{

	u_int32_t logcflag, max_fileno;
	DB_LOGC *logc;
	ENV *env;
	DBT data;
	DB_DISTAB dtab;
	DB_LSN key, start, start2, stop, stop2, verslsn;
	u_int32_t newversion, version;
	int cmp, fwdscroll, goprev, ret, tret;
	time_t starttime, endtime;
	const char *okmsg;
	DB_LOG_VRFY_INFO *logvrfy_hdl;

	okmsg = NULL;
	fwdscroll = 1;
	max_fileno = (u_int32_t)-1;
	goprev = 0;
	env = dbenv->env;
	logc = NULL;
	memset(&dtab, 0, sizeof(dtab));
	memset(&data, 0, sizeof(data));
	version = newversion = 0;
	ZERO_LSN(verslsn);
	memset(&start, 0, sizeof(DB_LSN));
	memset(&start2, 0, sizeof(DB_LSN));
	memset(&stop, 0, sizeof(DB_LSN));
	memset(&stop2, 0, sizeof(DB_LSN));
	memset(&key, 0, sizeof(DB_LSN));
	memset(&verslsn, 0, sizeof(DB_LSN));

	start = lvconfig->start_lsn;
	stop = lvconfig->end_lsn;
	starttime = lvconfig->start_time;
	endtime = lvconfig->end_time;

	if ((ret = __create_log_vrfy_info(lvconfig, &logvrfy_hdl, ip)) != 0)
		goto err;
	logvrfy_hdl->lv_config = lvconfig;
	if (lvconfig->continue_after_fail)
		F_SET(logvrfy_hdl, DB_LOG_VERIFY_CAF);
	if (lvconfig->verbose)
		F_SET(logvrfy_hdl, DB_LOG_VERIFY_VERBOSE);

	/* Allocate a log cursor. */
	if ((ret = __log_cursor(dbenv->env, &logc)) != 0) {
		__db_err(dbenv->env, ret, "DB_ENV->log_cursor");
		goto err;
	}
	/* Ignore failed chksum and go on with next one. */
	F_SET(logc->env->lg_handle, DBLOG_VERIFYING);

	/* Only scan the range that we want to verify. */
	if (fwdscroll) {
		if (IS_ZERO_LSN(stop)) {
			logcflag = DB_LAST;
			key.file = key.offset = 0;
		} else {
			key = stop;
			logcflag = DB_SET;
		}
		logvrfy_hdl->flags |= DB_LOG_VERIFY_FORWARD;
		goto startscroll;
	}

vrfyscroll:

	/*
	 * Initialize version to 0 so that we get the
	 * correct version right away.
	 */
	version = 0;
	ZERO_LSN(verslsn);

	/*
	 * In the log verification config struct, start_lsn and end_lsn have
	 * higher priority than start_time and end_time, and you can specify
	 * either lsn or time to start/stop verification.
	 */
	if (starttime != 0 || endtime != 0) {
		if ((ret = __find_lsnrg_by_timerg(logvrfy_hdl,
		    starttime, endtime, &start2, &stop2)) != 0)
			goto err;
		((DB_LOG_VERIFY_CONFIG *)lvconfig)->start_lsn = start = start2;
		((DB_LOG_VERIFY_CONFIG *)lvconfig)->end_lsn = stop = stop2;
	}

	if (IS_ZERO_LSN(start)) {
		logcflag = DB_FIRST;
		key.file = key.offset = 0;
	} else {
		key = start;
		logcflag = DB_SET;
		F_SET(logvrfy_hdl, DB_LOG_VERIFY_PARTIAL);
	}
	goprev = 0;

	/*
	 * So far we only support verifying a specific db file. The config's
	 * dbfile must be prefixed with the data directory if it's not in
	 * environment home directory.
	 */
	if (lvconfig->dbfile != NULL) {
		F_SET(logvrfy_hdl,
		    DB_LOG_VERIFY_DBFILE | DB_LOG_VERIFY_PARTIAL);
		if ((ret = __set_logvrfy_dbfuid(logvrfy_hdl)) != 0)
			goto err;
	}

startscroll:

	memset(&data, 0, sizeof(data));

	for (;;) {

		/*
		 * We may have reached beyond the range we're verifying.
		 */
		if (!fwdscroll && !IS_ZERO_LSN(stop)) {
			cmp = LOG_COMPARE(&key, &stop);
			if (cmp > 0)
				break;
		}
		if (fwdscroll && !IS_ZERO_LSN(start)) {
			cmp = LOG_COMPARE(&key, &start);
			if (cmp < 0)
				break;
		}

		ret = __logc_get(logc, &key, &data, logcflag);
		if (ret != 0) {
			if (ret == DB_NOTFOUND) {
				/* We may not start from the first log file. */
				if (logcflag == DB_PREV && key.file > 1)
					F_SET(logvrfy_hdl,
					    DB_LOG_VERIFY_PARTIAL);
				break;
			}
			__db_err(dbenv->env, ret, "DB_LOGC->get");
			/*
			 * When go beyond valid lsn range, we may get other
			 * error values than DB_NOTFOUND.
			 */
			goto out;
		}

		if (logcflag == DB_SET) {
			if (goprev)
				logcflag = DB_PREV;
			else
				logcflag = DB_NEXT;
		} else if (logcflag == DB_LAST) {
			logcflag = DB_PREV;
			max_fileno = key.file;
		} else if (logcflag == DB_FIRST)
			logcflag = DB_NEXT;

		if (key.file != verslsn.file) {
			/*
			 * If our log file changed, we need to see if the
			 * version of the log file changed as well.
			 * If it changed, reset the print table.
			 */
			if ((ret = __logc_version(logc, &newversion)) != 0) {
				__db_err(dbenv->env, ret, "DB_LOGC->version");
				goto err;
			}
			if (version != newversion) {
				version = newversion;
				if (!IS_LOG_VRFY_SUPPORTED(version)) {
					__db_msg(dbenv->env, DB_STR_A("2502",
				"[%lu][%lu] Unsupported version of log file, "
				"log file number: %u, log file version: %u, "
				"supported log version: %u.",
					    "%lu %lu %u %u %u"),
					    (u_long)key.file,
					    (u_long)key.offset,
					    key.file, version, DB_LOGVERSION);
					if (logcflag == DB_NEXT) {
						key.file += 1;
						if (key.file > max_fileno)
							break;
				/*
				 * Txns don't span log versions, no need to
				 * set DB_LOG_VERIFY_PARTIAL here.
				 */
					} else {
						goprev = 1;
						key.file -= 1;
						if (key.file == 0)
							break;
					}
					key.offset = FIRST_OFFSET(env);
					logcflag = DB_SET;
					continue;
				}
				if ((ret = __env_init_verify(env, version,
				    &dtab)) != 0) {
					__db_err(dbenv->env, ret,
					    DB_STR("2503",
					    "callback: initialization"));
					goto err;
				}
			}
			verslsn = key;
		}

		ret = __db_dispatch(dbenv->env, &dtab, &data, &key,
		    DB_TXN_LOG_VERIFY, logvrfy_hdl);

		if (!fwdscroll && ret != 0) {
			if (!F_ISSET(logvrfy_hdl, DB_LOG_VERIFY_CAF)) {
				__db_err(dbenv->env, ret,
				    "[%lu][%lu] __db_dispatch",
				    (u_long)key.file, (u_long)key.offset);
				goto err;
			} else
				F_SET(logvrfy_hdl, DB_LOG_VERIFY_ERR);
		}
	}

	if (fwdscroll) {
		fwdscroll = 0;
		F_CLR(logvrfy_hdl, DB_LOG_VERIFY_FORWARD);
		goto vrfyscroll;
	}
out:
	/*
	 * When we arrive here ret can be 0 or errors returned by DB_LOGC->get,
	 * all which we have already handled. So we clear ret.
	 */
	ret = 0;

	/* If continuing after fail, we can complete the entire log. */
	if (F_ISSET(logvrfy_hdl, DB_LOG_VERIFY_ERR) ||
	    F_ISSET(logvrfy_hdl, DB_LOG_VERIFY_INTERR))
		ret = DB_LOG_VERIFY_BAD;
	/*
	 * This function can be called when the environment is alive, so
	 * there can be active transactions.
	 */
	__db_log_verify_global_report(logvrfy_hdl);
	if (ret == DB_LOG_VERIFY_BAD)
		okmsg = DB_STR_P("FAILED");
	else {
		DB_ASSERT(dbenv->env, ret == 0);
		okmsg = DB_STR_P("SUCCEEDED");
	}

	__db_msg(dbenv->env, DB_STR_A("2504",
	    "Log verification ended and %s.", "%s"), okmsg);

err:
	if (logc != NULL)
		(void)__logc_close(logc);
	if ((tret = __destroy_log_vrfy_info(logvrfy_hdl)) != 0 && ret == 0)
		ret = tret;
	if (dtab.int_dispatch)
		__os_free(dbenv->env, dtab.int_dispatch);
	if (dtab.ext_dispatch)
		__os_free(dbenv->env, dtab.ext_dispatch);

	return (ret);
}

/*
 * __env_init_verify--
 */
static int
__env_init_verify(env, version, dtabp)
	ENV *env;
	u_int32_t version;
	DB_DISTAB *dtabp;
{
	int ret;

	/*
	 * We need to prime the print table with the current print
	 * functions.  Then we overwrite only specific entries based on
	 * each previous version we support.
	 */
	if ((ret = __bam_init_verify(env, dtabp)) != 0)
		goto err;
	if ((ret = __crdel_init_verify(env, dtabp)) != 0)
		goto err;
	if ((ret = __db_init_verify(env, dtabp)) != 0)
		goto err;
	if ((ret = __dbreg_init_verify(env, dtabp)) != 0)
		goto err;
	if ((ret = __fop_init_verify(env, dtabp)) != 0)
		goto err;
#ifdef HAVE_HASH
	if ((ret = __ham_init_verify(env, dtabp)) != 0)
		goto err;
#endif
#ifdef HAVE_HEAP
	if ((ret = __heap_init_verify(env, dtabp)) != 0)
		goto err;
#endif
#ifdef HAVE_QUEUE
	if ((ret = __qam_init_verify(env, dtabp)) != 0)
		goto err;
#endif
	if ((ret = __txn_init_verify(env, dtabp)) != 0)
		goto err;

	switch (version) {
	case DB_LOGVERSION:
		ret = 0;
		break;

	default:
		__db_errx(env, DB_STR_A("2505", "Not supported version %lu",
		    "%lu"), (u_long)version);
		ret = EINVAL;
		break;
	}
err:	return (ret);
}

/*
 * __log_verify_wrap --
 *      Wrapper function for APIs of other languages, like java/c# and
 *      script languages. It's much easier to implement the swig layer
 *      when we split up the C structure.
 *
 * PUBLIC: int __log_verify_wrap __P((ENV *, const char *, u_int32_t,
 * PUBLIC:     const char *, const char *, time_t, time_t, u_int32_t,
 * PUBLIC:     u_int32_t, u_int32_t, u_int32_t, int, int));
 */
int
__log_verify_wrap(env, envhome, cachesize, dbfile, dbname,
    stime, etime, stfile, stoffset, efile, eoffset, caf, verbose)
	ENV *env;
	const char *envhome, *dbfile, *dbname;
	time_t stime, etime;
	u_int32_t cachesize, stfile, stoffset, efile, eoffset;
	int caf, verbose;
{
	DB_LOG_VERIFY_CONFIG cfg;

	memset(&cfg, 0, sizeof(cfg));
	cfg.cachesize = cachesize;
	cfg.temp_envhome = envhome;
	cfg.dbfile = dbfile;
	cfg.dbname = dbname;
	cfg.start_time = stime;
	cfg.end_time = etime;
	cfg.start_lsn.file = stfile;
	cfg.start_lsn.offset = stoffset;
	cfg.end_lsn.file = efile;
	cfg.end_lsn.offset = eoffset;
	cfg.continue_after_fail = caf;
	cfg.verbose = verbose;

	return __log_verify_pp(env->dbenv, &cfg);
}
