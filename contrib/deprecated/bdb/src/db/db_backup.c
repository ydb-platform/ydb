/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/heap.h"
#include "dbinc/mp.h"
#include "dbinc/partition.h"

#ifdef HAVE_QUEUE
#include "dbinc/qam.h"
#endif

static void save_error __P((const DB_ENV *, const char *, const char *));
static int backup_read_log_dir __P((DB_ENV *, const char *, int *, u_int32_t));
static int backup_read_data_dir
    __P((DB_ENV *, DB_THREAD_INFO *, const char *, const char *, u_int32_t));
static int backup_dir_clean
    __P((DB_ENV *, const char *, const char *, int *, u_int32_t));
static int backup_data_copy
    __P((DB_ENV *, const char *, const char *, const char *, int));

/*
 * __db_dbbackup_pp --
 *	Copy a database file coordinated with mpool.
 *
 * PUBLIC: int __db_dbbackup_pp __P((DB_ENV *,
 * PUBLIC:     const char *, const char *, u_int32_t));
 */
int
__db_dbbackup_pp(dbenv, dbfile, target, flags)
	DB_ENV *dbenv;
	const char *dbfile, *target;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	int ret;

	if ((ret = __db_fchk(dbenv->env,
	    "DB_ENV->dbbackup", flags, DB_EXCL)) != 0)
		return (ret);
	ENV_ENTER(dbenv->env, ip);

	ret = __db_dbbackup(dbenv, ip, dbfile, target, flags);

	ENV_LEAVE(dbenv->env, ip);
	return (ret);
}

/*
 * __db_dbbackup --
 *	Copy a database file coordinated with mpool.
 *
 * PUBLIC: int __db_dbbackup __P((DB_ENV *, DB_THREAD_INFO *,
 * PUBLIC:     const char *, const char *, u_int32_t));
 */
int
__db_dbbackup(dbenv, ip, dbfile, target, flags)
	DB_ENV *dbenv;
	DB_THREAD_INFO *ip;
	const char *dbfile, *target;
	u_int32_t flags;
{
	DB *dbp;
	DB_FH *fp;
	void *handle;
	int ret, retry_count, t_ret;

	dbp = NULL;
	retry_count = 0;

retry:	if ((ret = __db_create_internal(&dbp, dbenv->env, 0)) == 0 &&
	    (ret = __db_open(dbp, ip, NULL, dbfile, NULL,
	    DB_UNKNOWN, DB_AUTO_COMMIT | DB_RDONLY, 0, PGNO_BASE_MD)) != 0) {
		if (ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED) {
			(void)__db_close(dbp, NULL, DB_NOSYNC);
			dbp = NULL;
			if (++retry_count > 100)
				return (ret);
			__db_errx(dbenv->env, DB_STR_A("0702",
		    "Deadlock while opening %s, retrying", "%s"), dbfile);
			__os_yield(dbenv->env, 1, 0);
			goto retry;
		}
	}

	if (ret == 0) {
		if ((ret = __memp_backup_open(dbenv->env,
		    dbp->mpf, dbfile, target, flags, &fp, &handle)) == 0) {
			if (dbp->type == DB_HEAP)
				ret = __heap_backup(
				    dbenv, dbp, ip, fp, handle, flags);
			else
				ret = __memp_backup_mpf(
				    dbenv->env, dbp->mpf,
				    ip, 0, dbp->mpf->mfp->last_pgno,
				    fp, handle, flags);
		}
		if ((t_ret = __memp_backup_close(dbenv->env,
		    dbp->mpf, dbfile, fp, handle)) != 0 && ret == 0)
			ret = t_ret;
	}

#ifdef HAVE_QUEUE
	/*
	 * For compatibility with the 5.2 and patch versions of db_copy
	 * dump the queue extents here.
	 */
	if (ret == 0 && dbp->type == DB_QUEUE)
		ret = __qam_backup_extents(dbp, ip, target, flags);
#endif

	if (dbp != NULL &&
	    (t_ret = __db_close(dbp, NULL, DB_NOSYNC)) != 0 && ret == 0)
		ret = t_ret;

	if (ret != 0)
		__db_err(dbenv->env, ret, "Backup Failed");
	return (ret);
}

/*
 * backup_dir_clean --
 *	Clean out the backup directory.
 */
static int
backup_dir_clean(dbenv, backup_dir, log_dir, remove_maxp, flags)
	DB_ENV *dbenv;
	const char *backup_dir, *log_dir;
	int *remove_maxp;
	u_int32_t flags;
{
	ENV *env;
	int cnt, fcnt, ret, v;
	const char *dir;
	char **names, buf[DB_MAXPATHLEN], path[DB_MAXPATHLEN];

	env = dbenv->env;

	/* We may be cleaning a log directory separate from the target. */
	if (log_dir != NULL) {
		if ((ret = __os_concat_path(buf,
		    sizeof(buf), backup_dir, log_dir)) != 0) {
			buf[sizeof(buf) - 1] = '\0';
			__db_errx(env,  DB_STR_A("0717",
			    "%s: path too long", "%s"), buf);
			return (EINVAL);
		}
		dir = buf;
	} else
		dir = backup_dir;

	/* Get a list of file names. */
	if ((ret = __os_dirlist(env, dir, 0, &names, &fcnt)) != 0) {
		if (log_dir != NULL && !LF_ISSET(DB_BACKUP_UPDATE))
			return (0);
		__db_err(env,
		    ret, DB_STR_A("0718", "%s: directory read", "%s"), dir);
		return (ret);
	}
	for (cnt = fcnt; --cnt >= 0;) {
		/*
		 * Skip non-log files (if update was specified).
		 */
		if (!IS_LOG_FILE(names[cnt])) {
			if (LF_ISSET(DB_BACKUP_UPDATE))
				continue;
		} else {
			/* Track the highest-numbered log file removed. */
			v = atoi(names[cnt] + sizeof(LFPREFIX) - 1);
			if (*remove_maxp < v)
				*remove_maxp = v;
		}
		if ((ret = __os_concat_path(path,
		    sizeof(path), dir, names[cnt])) != 0) {
			path[sizeof(path) - 1] = '\0';
			__db_errx(env, DB_STR_A("0714",
			    "%s: path too long", "%s"), path);
			return (EINVAL);
		}
		if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
			__db_msg(env, DB_STR_A("0715", "removing %s",
			    "%s"),  path);
		if ((ret = __os_unlink(env, path, 0)) != 0)
			return (ret);
	}

	__os_dirfree(env, names, fcnt);

	if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP) && *remove_maxp != 0)
		__db_msg(env, DB_STR_A("0719",
		    "highest numbered log file removed: %d", "%d"),
		    *remove_maxp);

	return (0);
}

/*
 * backup_data_copy --
 *	Copy a non-database file into the backup directory.
 */
static int
backup_data_copy(dbenv, file, from_dir, to_dir, log)
	DB_ENV *dbenv;
	const char *file, *from_dir, *to_dir;
	int log;
{
	DB_BACKUP *backup;
	DB_FH *rfhp, *wfhp;
	ENV *env;
	u_int32_t gigs, off;
	size_t nr, nw;
	int ret, t_ret;
	char *buf;
	void *handle;
	char from[DB_MAXPATHLEN], to[DB_MAXPATHLEN];

	rfhp = wfhp = NULL;
	handle = NULL;
	buf = NULL;
	env = dbenv->env;
	backup = env->backup_handle;

	if ((ret = __os_concat_path(from,
	    sizeof(from), from_dir, file)) != 0) {
		from[sizeof(from) - 1] = '\0';
		__db_errx(env, DB_STR_A("0728",
		     "%s: path too long", "%s"), from);
		goto err;
	}
	if ((ret = __os_concat_path(to,
	    sizeof(to), to_dir, file)) != 0) {
		to[sizeof(to) - 1] = '\0';
		__db_errx(env, DB_STR_A("0729",
		     "%s: path too long", "%s"), to);
		goto err;
	}
	if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
		__db_msg(env, DB_STR_A("0726",
		    "copying %s to %s", "%s %s"), from, to);

	if ((ret = __os_malloc(env, MEGABYTE, &buf)) != 0) {
		__db_err(env, ret, DB_STR_A("0727",
		    "%lu buffer allocation", "%lu"), (u_long)MEGABYTE);
		return (ret);
	}

	/* Open the input file. */
	if ((ret = __os_open(env, from, 0, DB_OSO_RDONLY, 0, &rfhp)) != 0) {
		if (ret == ENOENT && !log) {
			ret = 0;
			if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
				__db_msg(env, DB_STR_A("0730",
				    "%s%c%s not present", "%s %c %s"),
				    from_dir, PATH_SEPARATOR[0], file);
			goto done;
		}
		__db_err(env, ret, "%s", buf);
		goto err;
	}

	/* Open the output file. */
	if (backup != NULL && backup->open != NULL)
		ret = backup->open(env->dbenv, file, to_dir, &handle);
	else {
		if ((ret = __os_open(env, to, 0,
		    DB_OSO_CREATE | DB_OSO_TRUNC, DB_MODE_600, &wfhp)) != 0) {
			__db_err(env, ret, "%s", to);
			goto err;
		}
	}

	off = 0;
	gigs = 0;
	/* Copy the data. */
	while ((ret = __os_read(env, rfhp, buf, MEGABYTE, &nr)) == 0 &&
	    nr > 0) {
		if (backup != NULL && backup->write != NULL) {
			if ((ret = backup->write(env->dbenv, gigs,
			     off, (u_int32_t)nr, (u_int8_t *)buf, handle)) != 0)
				break;
		} else {
			if ((ret = __os_write(env, wfhp, buf, nr, &nw)) != 0)
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
	}
	if (ret != 0)
		__db_err(env, ret, DB_STR("0748", "Write failed."));

err:
done:	if (buf != NULL)
		__os_free(env, buf);

	if (backup != NULL && backup->close != NULL &&
	    (t_ret = backup->close(env->dbenv, file, handle)) != 0 && ret != 0)
		ret = t_ret;
	if (rfhp != NULL &&
	    (t_ret = __os_closehandle(env, rfhp)) != 0 && ret == 0)
		ret = t_ret;

	/* We may be running on a remote filesystem; force the flush. */
	if (ret == 0 && wfhp != NULL) {
		ret = __os_fsync(env, wfhp);
		if (ret != 0)
			__db_err(env, ret, DB_STR("0731", "Sync failed"));
	}
	if (wfhp != NULL &&
	    (t_ret = __os_closehandle(env, wfhp)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static void save_error(dbenv, prefix, errstr)
	const DB_ENV *dbenv;
	const char *prefix;
	const char *errstr;
{
	COMPQUIET(prefix, NULL);
	if (DB_GLOBAL(saved_errstr) != NULL)
		__os_free(dbenv->env, DB_GLOBAL(saved_errstr));
	(void)__os_strdup(dbenv->env, errstr, &DB_GLOBAL(saved_errstr));
}

/*
 * backup_read_data_dir --
 *	Read a directory looking for databases to copy.
 */
static int
backup_read_data_dir(dbenv, ip, dir, backup_dir, flags)
	DB_ENV *dbenv;
	DB_THREAD_INFO *ip;
	const char *dir, *backup_dir;
	u_int32_t flags;
{
	DB_MSGBUF mb;
	ENV *env;
	FILE *savefile;
	int fcnt, ret;
	size_t cnt;
	const char *bd;
	char **names, buf[DB_MAXPATHLEN], bbuf[DB_MAXPATHLEN];
	void (*savecall) (const DB_ENV *, const char *, const char *);

	env = dbenv->env;
	memset(bbuf, 0, sizeof(bbuf));

	bd = backup_dir;
	if (!LF_ISSET(DB_BACKUP_SINGLE_DIR) && dir != env->db_home) {
		cnt = sizeof(bbuf);
		/* Build a path name to the destination. */
		if ((ret = __os_concat_path(bbuf, sizeof(bbuf),
		    backup_dir, dir)) != 0 ||
		    (((cnt = strlen(bbuf)) == sizeof(bbuf) ||
		    (cnt == sizeof(bbuf) - 1 &&
		    strchr(PATH_SEPARATOR, bbuf[cnt - 1]) == NULL)) &&
		    LF_ISSET(DB_CREATE))) {
			bbuf[sizeof(bbuf) - 1] = '\0';
			__db_errx(env, DB_STR_A("0720",
			    "%s: path too long", "%s"), bbuf);
			return (1);
		}

		/* Create the path. */
		if (LF_ISSET(DB_CREATE)) {
			if (strchr(PATH_SEPARATOR, bbuf[cnt - 1]) == NULL)
				bbuf[cnt] = PATH_SEPARATOR[0];

			if ((ret = __db_mkpath(env, bbuf)) != 0) {
				__db_err(env,  ret, DB_STR_A("0721",
				    "%s: cannot create", "%s"), bbuf);
				return (ret);
			}
			/* step on the trailing '/' */
			bbuf[cnt] = '\0';
		}
		bd = bbuf;

	}
	if (!__os_abspath(dir) && dir != env->db_home) {
		/* Build a path name to the source. */
		if ((ret = __os_concat_path(buf,
		    sizeof(buf), env->db_home, dir)) != 0) {
			buf[sizeof(buf) - 1] = '\0';
			__db_errx(env, DB_STR_A("0722",
			    "%s: path too long", "%s"), buf);
			return (EINVAL);
		}
		dir = buf;
	}
	/* Get a list of file names. */
	if ((ret = __os_dirlist(env, dir, 0, &names, &fcnt)) != 0) {
		__db_err(env, ret, DB_STR_A("0723", "%s: directory read",
		    "%s"), dir);
		return (ret);
	}
	for (cnt = (size_t)fcnt; cnt-- > 0;) {
		/*
		 * Skip files in DB's name space, except replication dbs.
		 */
		if (IS_LOG_FILE(names[cnt]))
			continue;
		if (IS_DB_FILE(names[cnt]) && !IS_REP_FILE(names[cnt])
#ifdef HAVE_PARTITION
		    && !IS_PARTITION_DB_FILE(names[cnt])
#endif
		)
			continue;

		/*
		 * Skip DB_CONFIG.
		 */
		if (LF_ISSET(DB_BACKUP_SINGLE_DIR) &&
		     !strncmp(names[cnt], "DB_CONFIG", sizeof("DB_CONFIG")))
			continue;

		/*
		 * Copy the database.
		 */

		DB_MSGBUF_INIT(&mb);
		if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
			__db_msgadd(env, &mb, DB_STR_A("0724",
			    "copying database %s%c%s to %s%c%s",
			    "%s%c%s %s%c%s"),
			    dir, PATH_SEPARATOR[0], names[cnt],
			    bd, PATH_SEPARATOR[0], names[cnt]);

		/*
		 * Suppress errors on non-db files.
		 */
		savecall = dbenv->db_errcall;
		dbenv->db_errcall = save_error;
		savefile = dbenv->db_errfile;
		dbenv->db_errfile = NULL;

		ret = __db_dbbackup(dbenv, ip, names[cnt], bd, flags);

		dbenv->db_errcall = savecall;
		dbenv->db_errfile = savefile;

		/* The file might not be a database. */
		if (ret == ENOENT || ret == EINVAL) {
			if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP)) {
				__db_msgadd(env, &mb, " -- Not a database");
				DB_MSGBUF_FLUSH(env, &mb);
			}
			if (LF_ISSET(DB_BACKUP_FILES))
				ret = backup_data_copy(
				    dbenv, names[cnt], dir, bd, 0);
			else
				ret = 0;
		} else if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
			DB_MSGBUF_FLUSH(env, &mb);

		if (ret != 0) {
			if (DB_GLOBAL(saved_errstr) != NULL) {
				__db_errx(env, "%s", DB_GLOBAL(saved_errstr));
				__os_free(env, DB_GLOBAL(saved_errstr));
				DB_GLOBAL(saved_errstr) = NULL;
			}
			break;
		}
	}

	__os_dirfree(env, names, fcnt);

	return (ret);
}

/*
 * backup_read_log_dir --
 *	Read a directory looking for log files to copy.
 */
static int
backup_read_log_dir(dbenv, backup_dir, copy_minp, flags)
	DB_ENV *dbenv;
	const char *backup_dir;
	int *copy_minp;
	u_int32_t flags;
{
	ENV *env;
	u_int32_t aflag;
	size_t cnt;
	int ret, update, v;
	const char *backupd;
	char **begin, **names, *logd;
	char from[DB_MAXPATHLEN], to[DB_MAXPATHLEN];

	env = dbenv->env;
	ret = 0;
	begin = NULL;
	memset(to, 0, sizeof(to));

	/*
	 * Figure out where the log files are and create the log
	 * destination directory if necessary.
	 */
	backupd = backup_dir;
	if ((logd = dbenv->db_log_dir) == NULL)
		logd = env->db_home;
	else {
		if (!LF_ISSET(DB_BACKUP_SINGLE_DIR)) {
			cnt = sizeof(to);
			if ((ret = __os_concat_path(to,
			    sizeof(to), backup_dir, logd)) != 0 ||
			    (((cnt = strlen(to)) == sizeof(to) ||
			    (cnt == sizeof(to) - 1 &&
			    strchr(PATH_SEPARATOR, to[cnt - 1]) == NULL)) &&
			    LF_ISSET(DB_CREATE))) {
				to[sizeof(to) - 1] = '\0';
				__db_errx(env, DB_STR_A("0733",
				    "%s: path too long", "%s"), to);
				goto err;
			}
			if (LF_ISSET(DB_CREATE)) {
				if (strchr(PATH_SEPARATOR, to[cnt - 1]) == NULL)
					to[cnt] = PATH_SEPARATOR[0];

				if ((ret = __db_mkpath(env, to)) != 0) {
					__db_err(env, ret, DB_STR_A("0734",
					    "%s: cannot create", "%s"), to);
					goto err;
				}
				to[cnt] = '\0';
			}
			if ((ret = __os_strdup(env, to, (void*) &backupd)) != 0)
				goto err;
		}
		if (!__os_abspath(logd)) {
			if ((ret = __os_concat_path(from,
			    sizeof(from), env->db_home, logd)) != 0) {
				from[sizeof(from) - 1] = '\0';
				__db_errx(env, DB_STR_A("0732",
				    "%s: path too long", "%s"), from);
				goto err;
			}
			if ((ret = __os_strdup(env, from, &logd)) != 0)
				goto err;
		}
	}

	update = LF_ISSET(DB_BACKUP_UPDATE);
again:	aflag = DB_ARCH_LOG;

	/*
	 * If this is an update and we are deleting files, first process
	 * those files that can be removed, then repeat with the rest.
	 */
	if (update)
		aflag = 0;

	/* Flush the log to get latest info. */
	if ((ret = __log_flush(env, NULL)) != 0) {
		__db_err(env, ret, DB_STR("0735", "Can't flush log"));
		goto err;
	}

	/* Get a list of file names to be copied. */
	if ((ret = __log_archive(env, &names, aflag)) != 0) {
		__db_err(env, ret, DB_STR("0736", "Can't get log file names"));
		goto err;
	}
	if (names == NULL)
		goto done;
	begin = names;
	for (; *names != NULL; names++) {
		/* Track the lowest-numbered log file copied. */
		v = atoi(*names + sizeof(LFPREFIX) - 1);
		if (*copy_minp == 0 || *copy_minp > v)
			*copy_minp = v;

		if ((ret = __os_concat_path(from,
		    sizeof(from), logd, *names)) != 0) {
			from[sizeof(from) - 1] = '\0';
			__db_errx(env, DB_STR_A("0737",
			    "%s: path too long", "%s"), from);
			goto err;
		}

		/*
		 * If we're going to remove the file, attempt to rename it
		 * instead of copying and then removing.  The likely failure
		 * is EXDEV (source and destination are on different volumes).
		 * Fall back to a copy, regardless of the error.  We don't
		 * worry about partial contents, the copy truncates the file
		 * on open.
		 */
		if (update) {
			if ((ret = __os_concat_path(to,
			    sizeof(to), backupd, *names)) != 0) {
				to[sizeof(to) - 1] = '\0';
				__db_errx(env, DB_STR_A("0738",
				    "%s: path too long", "%s"), to);
				goto err;
			}
			if (__os_rename(env, from, to, 1) == 0) {
				if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
					__db_msg(env, DB_STR_A("0739",
					    "moving %s to %s",
					    "%s %s"), from, to);
				continue;
			}
		}

		/* Copy the file. */
		if (backup_data_copy(dbenv, *names, logd, backupd, 1) != 0) {
			ret = 1;
			goto err;
		}

		if (update) {
			if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP))
				__db_msg(env, DB_STR_A("0740",
				    "removing %s", "%s"), from);
			if ((ret = __os_unlink(env, from, 0)) != 0) {
				__db_err(env, ret, DB_STR_A("0741",
				    "unlink of %s failed", "%s"), from);
				goto err;
			}
		}

	}

	__os_ufree(env, begin);
	begin = NULL;
done:	if (update) {
		update = 0;
		goto again;
	}

	if (FLD_ISSET(dbenv->verbose, DB_VERB_BACKUP) && *copy_minp != 0)
		__db_msg(env, DB_STR_A("0742",
		    "lowest numbered log file copied: %d", "%d"),
		    *copy_minp);
err:	if (logd != dbenv->db_log_dir && logd != env->db_home)
		__os_free(env, logd);
	if (backupd != NULL && backupd != backup_dir)
		__os_free(env, (void *)backupd);
	if (begin != NULL)
		__os_ufree(env, begin);

	return (ret);
}

/*
 * __db_backup --
 *	Backup databases in the enviornment.
 *
 * PUBLIC: int __db_backup __P((DB_ENV *, const char *, u_int32_t));
 */
int
__db_backup(dbenv, target, flags)
	DB_ENV *dbenv;
	const char *target;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int copy_min, remove_max, ret;
	char **dir;

	env = dbenv->env;
	remove_max = copy_min = 0;

#undef	OKFLAGS
#define	OKFLAGS								\
	(DB_CREATE | DB_EXCL | DB_BACKUP_FILES | DB_BACKUP_SINGLE_DIR |	\
	DB_BACKUP_UPDATE | DB_BACKUP_NO_LOGS | DB_BACKUP_CLEAN)

	if ((ret = __db_fchk(env, "DB_ENV->backup", flags, OKFLAGS)) != 0)
		return (ret);

	if (target == NULL) {
		__db_errx(env,
		    DB_STR("0716", "Target directory may not be null."));
		return (EINVAL);
	}

	/*
	 * If the target directory for the backup does not exist, create it
	 * with mode read-write-execute for the owner.  Ignore errors here,
	 * it's simpler and more portable to just always try the create.  If
	 * there's a problem, we'll fail with reasonable errors later.
	 */
	if (LF_ISSET(DB_CREATE))
		(void)__os_mkdir(NULL, target, DB_MODE_700);

	if (LF_ISSET(DB_BACKUP_CLEAN)) {
		if (!LF_ISSET(DB_BACKUP_SINGLE_DIR) &&
		    dbenv->db_log_dir != NULL &&
		    (ret = backup_dir_clean(dbenv, target,
		    dbenv->db_log_dir, &remove_max, flags)) != 0)
			return (ret);
		if ((ret = backup_dir_clean(dbenv,
		    target, NULL, &remove_max, flags)) != 0)
			return (ret);

	}

	ENV_ENTER(env, ip);

	/*
	 * If the UPDATE option was not specified, copy all database
	 * files found in the database environment home directory and
	 * data directories..
	 */
	if ((ret = __env_set_backup(env, 1)) != 0)
		goto end;
	F_SET(dbenv, DB_ENV_HOTBACKUP);
	if (!LF_ISSET(DB_BACKUP_UPDATE)) {
		if ((ret = backup_read_data_dir(dbenv,
		    ip, env->db_home, target, flags)) != 0)
			goto err;
		for (dir = dbenv->db_data_dir;
		    dir != NULL && *dir != NULL; ++dir) {
			/*
			 * Don't allow absolute path names taken from the
			 * enviroment  -- running recovery with them would
			 * corrupt the source files.
			 */
			if (!LF_ISSET(DB_BACKUP_SINGLE_DIR)
			   && __os_abspath(*dir)) {
				__db_errx(env, DB_STR_A("0725",
"data directory '%s' is absolute path, not permitted unless backup is to a single directory",
				    "%s"), *dir);
				ret = EINVAL;
				goto err;
			}
			if ((ret = backup_read_data_dir(
			    dbenv, ip, *dir, target, flags)) != 0)
				goto err;
		}
	}

	/*
	 * Copy all log files found in the log directory.
	 * The log directory defaults to the home directory.
	 */
	if ((ret = backup_read_log_dir(dbenv, target, &copy_min, flags)) != 0)
		goto err;
	/*
	 * If we're updating a snapshot, the lowest-numbered log file copied
	 * into the backup directory should be less than, or equal to, the
	 * highest-numbered log file removed from the backup directory during
	 * cleanup.
	 */
	if (LF_ISSET(DB_BACKUP_UPDATE) && remove_max < copy_min &&
	     !(remove_max == 0 && copy_min == 1)) {
		__db_errx(env, DB_STR_A("0743",
"the largest log file removed (%d) must be greater than or equal the smallest log file copied (%d)",
		    "%d %d"), remove_max, copy_min);
		ret = EINVAL;
	}

err:	F_CLR(dbenv, DB_ENV_HOTBACKUP);
	(void)__env_set_backup(env, 0);
end:	ENV_LEAVE(env, ip);
	return (ret);
}
