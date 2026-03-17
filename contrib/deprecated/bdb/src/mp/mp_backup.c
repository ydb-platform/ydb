/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/mp.h"
#ifndef HAVE_ATOMICFILEREAD
#include "dbinc/db_page.h"
#endif

#ifndef HAVE_ATOMICFILEREAD
static int __memp_check_backup __P((ENV *,
    MPOOLFILE *, void *, u_int32_t *, u_int32_t));
#endif

/*
 * __memp_backup_open --
 *	Setup to backup a database file.
 *
 * PUBLIC: int __memp_backup_open __P((ENV *, DB_MPOOLFILE *,
 * PUBLIC:     const char *, const char *, u_int32_t, DB_FH **, void**));
 */
int
__memp_backup_open(env, mpf, dbfile, target, flags, fpp, handlep)
	ENV *env;
	DB_MPOOLFILE *mpf;
	const char *dbfile;
	const char *target;
	u_int32_t flags;
	DB_FH **fpp;
	void **handlep;
{
	DB_BACKUP *backup;
#ifndef HAVE_ATOMICFILEREAD
	MPOOLFILE *mfp;
#endif
	u_int32_t oflags;
	size_t len;
	int ret;
	char *path;

	path = NULL;
	*fpp = NULL;
	backup = env->backup_handle;
	*handlep = NULL;

	if (backup != NULL && backup->open != NULL)
		ret = backup->open(env->dbenv, dbfile, target, handlep);
	else {
		len = strlen(target) + strlen(dbfile) + 2;
		if ((ret = __os_malloc(env, len, &path)) != 0) {
			__db_err(env, ret, DB_STR_A("0703",
			    "Cannot allocate space for path: %s", "%s"),
			    target);
			goto err;
		}

		if ((ret = __os_concat_path(path, len, target, dbfile)) != 0)
			goto err;

		oflags = DB_OSO_CREATE | DB_OSO_TRUNC;
		if (LF_ISSET(DB_EXCL))
			FLD_SET(oflags, DB_OSO_EXCL);
		if (backup != NULL && F_ISSET(backup, BACKUP_WRITE_DIRECT))
			FLD_SET(oflags, DB_OSO_DIRECT);
		ret = __os_open(env, path, 0, oflags, DB_MODE_600, fpp);
	}
	if (ret != 0) {
		__db_err(env, ret, DB_STR_A("0704",
		    "Cannot open target file: %s", "%s"), path);
		goto err;
	}

#ifndef HAVE_ATOMICFILEREAD
	mfp = mpf->mfp;

	/*
	 * Need to register thread with fail check.
	 */
	MUTEX_LOCK(env, mfp->mtx_write);
	if (mfp->backup_in_progress) {
		__db_err(env, ret, DB_STR_A("0712",
		    "%s is already in a backup", "%s"), dbfile);
		MUTEX_UNLOCK(env, mfp->mtx_write);
		goto err;
	}
	mfp->backup_in_progress = 1;
	env->dbenv->thread_id(env->dbenv, &mfp->pid, &mfp->tid);
	MUTEX_UNLOCK(env, mfp->mtx_write);
#else
	COMPQUIET(mpf, NULL);
#endif
err:	if (path != NULL)
		__os_free(env, path);
	if (ret != 0) {
		if (*fpp != NULL)
			(void)__os_closehandle(env, *fpp);
		if (backup != NULL && backup->close != NULL)
			(void)backup->close(env->dbenv, dbfile, *handlep);
	}
	return (ret);
}

/*
 * __memp_backup_mpf --
 *	Copy a database file while maintaining synchronization with
 * mpool write activity.
 *
 * PUBLIC: int __memp_backup_mpf __P((ENV *, DB_MPOOLFILE *, DB_THREAD_INFO *,
 * PUBLIC:     db_pgno_t, db_pgno_t, DB_FH *, void *,  u_int32_t));
 */
int
__memp_backup_mpf(env, mpf, ip, first_pgno, last_pgno, fp, handle, flags)
	ENV *env;
	DB_MPOOLFILE *mpf;
	DB_THREAD_INFO *ip;
	db_pgno_t first_pgno, last_pgno;
	DB_FH *fp;
	void *handle;
	u_int32_t flags;
{
	DB_BACKUP *backup;
	MPOOLFILE *mfp;
	db_pgno_t high_pgno, pgno;
	off_t t_off;
	u_int32_t read_count, write_size;
	u_int32_t gigs, off;
	size_t len, nr, nw;
	u_int8_t *buf;
	int ret;

	COMPQUIET(flags, 0);
	backup = env->backup_handle;
	read_count = 0;
	buf = NULL;
	mfp = mpf->mfp;
	gigs = 0;
	off = 0;

	if (backup == NULL || (len = backup->size) == 0)
		len = MEGABYTE;
	if ((ret = __os_malloc(env, len, &buf)) != 0)
		return (ret);
	write_size = (u_int32_t)(len / mfp->pagesize);

	if (first_pgno > 0) {
		t_off = (off_t)first_pgno * mfp->pagesize;
		gigs = (u_int32_t)(t_off / GIGABYTE);
		off = (u_int32_t)(t_off - (off_t)gigs * GIGABYTE);
	}

	for (pgno = first_pgno; pgno <= last_pgno; pgno = high_pgno + 1) {
		high_pgno = pgno + write_size - 1;
		if (high_pgno > last_pgno)
			high_pgno = last_pgno;
		len = ((high_pgno - pgno) + 1) * mfp->pagesize;
#ifndef HAVE_ATOMICFILEREAD
		if (ip != NULL)
			ip->dbth_state = THREAD_ACTIVE;
		MUTEX_LOCK(env, mfp->mtx_write);

		/* Eventually the writers will drain and block on the mutex. */
		while (atomic_read(&mfp->writers) != 0) {
			STAT_INC_VERB(env, mpool, backup_spins,
			     mfp->stat.st_backup_spins, __memp_fn(mpf), pgno);
			__os_yield(env, 0, 1000);
		}

		mfp->low_pgno = pgno;
		mfp->high_pgno = high_pgno;
		MUTEX_UNLOCK(env, mfp->mtx_write);
		if (ip != NULL)
			ip->dbth_state = THREAD_OUT;
#endif

		if ((ret = __os_io(env, DB_IO_READ, mpf->fhp, pgno,
		    mfp->pagesize, 0, (u_int32_t)len, buf, &nr)) != 0)
			break;

		if (nr == 0)
			break;

		if (backup != NULL && backup->write != NULL) {
			if ((ret = backup->write(
			     env->dbenv, gigs, off, (u_int32_t)nr, 
			     buf, handle)) != 0)
				break;
		} else {
			if ((ret = __os_io(env, DB_IO_WRITE, fp, pgno,
			    mfp->pagesize, 0, (u_int32_t)nr, buf, &nw)) != 0)
				break;
			if (nr != nw) {
				ret = EIO;
				break;
			}
		}

		off += (u_int32_t)nr;
		if (off >= GIGABYTE) {
			gigs++;
			off -= GIGABYTE;
		}

		if (backup != NULL && backup->read_count != 0) {
			if ((read_count += write_size) >= backup->read_count)
				__os_yield(env, 0, backup->read_sleep);
		}

		/*
		 * There may be pages not written to the file yet.  The
		 * next read will probably see the end of file.
		 */
		if (nr != len)
			high_pgno = pgno + (db_pgno_t)(nr / mfp->pagesize);
	}
	DB_ASSERT(env, ret == 0);
	__os_free(env, buf);

#ifndef HAVE_ATOMICFILEREAD
	if (ip != NULL)
		ip->dbth_state = THREAD_ACTIVE;
	MUTEX_LOCK(env, mfp->mtx_write);
	mfp->low_pgno = PGNO_INVALID;
	mfp->high_pgno = PGNO_INVALID;
	MUTEX_UNLOCK(env, mfp->mtx_write);
#else
	COMPQUIET(ip, NULL);
#endif

	return (ret);
}

/*
 * __memp_backup_close --
 *	Close backup file.
 *
 * PUBLIC: int __memp_backup_close __P((ENV *, DB_MPOOLFILE *,
 * PUBLIC:	const char *, DB_FH *, void *HANDLE));
 */
int
__memp_backup_close(env, mpf, dbfile, fp, handle)
	ENV *env;
	DB_MPOOLFILE *mpf;
	const char *dbfile;
	DB_FH *fp;
	void *handle;
{
	DB_BACKUP *backup;
#ifndef HAVE_ATOMICFILEREAD
	MPOOLFILE *mfp;
#endif
	int ret, t_ret;

	backup = env->backup_handle;
	ret = t_ret = 0;

#ifndef HAVE_ATOMICFILEREAD
	mfp = mpf->mfp;
	MUTEX_LOCK(env, mfp->mtx_write);
	mfp->backup_in_progress = 0;
	MUTEX_UNLOCK(env, mfp->mtx_write);
#else
	COMPQUIET(mpf, NULL);
#endif
	if (fp != NULL)
		ret = __os_closehandle(env, fp);
	if (backup != NULL && backup->close != NULL)
		t_ret = backup->close(env->dbenv, dbfile, handle);
	return (ret == 0 ? t_ret : ret);
}

#ifndef HAVE_ATOMICFILEREAD
/*
 * __memp_check_backup --
 *	check for a dead thread backing up a mp file.
 */
static int
__memp_check_backup(env, mfp, arg, countp, flags)
	ENV *env;
	MPOOLFILE *mfp;
	void *arg;
	u_int32_t *countp;
	u_int32_t flags;
{
	DB_ENV *dbenv;
	char buf[DB_THREADID_STRLEN];

	COMPQUIET(arg, NULL);
	COMPQUIET(countp, NULL);
	COMPQUIET(flags, 0);

	dbenv = env->dbenv;

	if (mfp->backup_in_progress == 0 ||
	    dbenv->is_alive(dbenv, mfp->pid, mfp->tid, 0))
		return (0);

	__db_msg(env, DB_STR_A("3042", "Releasing backup of %s for %s.",
	    "%s %s"), (char *)R_ADDR(env->mp_handle->reginfo, mfp->path_off),
	    dbenv->thread_id_string(dbenv, mfp->pid, mfp->tid, buf));
	mfp->backup_in_progress = 0;
	return (0);
}
#endif

/*
 * __memp_failchk --
 *	Remove in process database backups.
 * PUBLIC: int __memp_failchk __P((ENV *));
 */
int
__memp_failchk(env)
	ENV *env;
{
#ifdef HAVE_ATOMICFILEREAD
	COMPQUIET(env, NULL);
	return (0);
#else
	DB_MPOOL *dbmp;
	MPOOL *mp;

	dbmp = env->mp_handle;
	mp = dbmp->reginfo[0].primary;

	return (__memp_walk_files(env, mp, __memp_check_backup, NULL, NULL, 0));
#endif
}
