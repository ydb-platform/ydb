/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef _DB_GLOBALS_H_
#define	_DB_GLOBALS_H_

#if defined(__cplusplus)
extern "C" {
#endif

/*******************************************************
 * Global variables.
 *
 * Held in a single structure to minimize the name-space pollution.
 *******************************************************/
#ifdef HAVE_VXWORKS
#error #include "semLib.h"
#endif

typedef struct __db_globals {
#ifdef HAVE_VXWORKS
	u_int32_t db_global_init;	/* VxWorks: inited */
	SEM_ID db_global_lock;		/* VxWorks: global semaphore */
#endif
#ifdef DB_WIN32
#ifndef DB_WINCE
	/*
	 * These fields are used by the Windows implementation of mutexes.
	 * Usually they are initialized by the first DB API call to lock a
	 * mutex. If that would result in the mutexes being inaccessible by
	 * other threads (e.g., ones which have lesser privileges) the
	 * application may first call db_env_set_win_security().
	 */
	SECURITY_DESCRIPTOR win_default_sec_desc;
	SECURITY_ATTRIBUTES win_default_sec_attr;
#endif
	SECURITY_ATTRIBUTES *win_sec_attr;
#endif
	
	/* TAILQ_HEAD(__envq, __dbenv) envq; */
	struct __envq {
		struct __env *tqh_first;
		struct __env **tqh_last;
	} envq;

	char *db_line;			/* DB display string. */

	char error_buf[40];		/* Error string buffer. */

	int uid_init;			/* srand set in UID generator */

	u_long rand_next;		/* rand/srand value */

	u_int32_t fid_serial;		/* file id counter */

	int db_errno;			/* Errno value if not available */

	size_t num_active_pids;		/* number of entries in active_pids */

	size_t size_active_pids;	/* allocated size of active_pids */

	pid_t *active_pids;		/* array active pids */

	char *saved_errstr;		/* saved error string from backup */

	/* Underlying OS interface jump table.*/
	void	(*j_assert) __P((const char *, const char *, int));
	int	(*j_close) __P((int));	
	void	(*j_dirfree) __P((char **, int));
	int	(*j_dirlist) __P((const char *, char ***, int *));
	int	(*j_exists) __P((const char *, int *));
	void	(*j_free) __P((void *));
	int	(*j_fsync) __P((int));
	int	(*j_ftruncate) __P((int, off_t));
	int	(*j_ioinfo) __P((const char *,
		    int, u_int32_t *, u_int32_t *, u_int32_t *));
	void   *(*j_malloc) __P((size_t));
	int	(*j_file_map) __P((DB_ENV *, char *, size_t, int, void **));
	int	(*j_file_unmap) __P((DB_ENV *, void *));
	int	(*j_open) __P((const char *, int, ...));
	ssize_t	(*j_pread) __P((int, void *, size_t, off_t));
	ssize_t	(*j_pwrite) __P((int, const void *, size_t, off_t));
	ssize_t	(*j_read) __P((int, void *, size_t));
	void   *(*j_realloc) __P((void *, size_t));
	int	(*j_region_map) __P((DB_ENV *, char *, size_t, int *, void **));
	int	(*j_region_unmap) __P((DB_ENV *, void *));
	int	(*j_rename) __P((const char *, const char *));
	int	(*j_seek) __P((int, off_t, int));
	int	(*j_unlink) __P((const char *));
	ssize_t	(*j_write) __P((int, const void *, size_t));
	int	(*j_yield) __P((u_long, u_long));
} DB_GLOBALS;

extern	DB_GLOBALS	__db_global_values;
#define	DB_GLOBAL(v)	__db_global_values.v

#if defined(__cplusplus)
}
#endif
#endif /* !_DB_GLOBALS_H_ */
