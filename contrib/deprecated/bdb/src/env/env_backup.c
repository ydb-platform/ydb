/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

static int __env_backup_alloc __P((DB_ENV *));

static int
__env_backup_alloc(dbenv)
	DB_ENV *dbenv;
{
	ENV *env;

	env = dbenv->env;
	if (env->backup_handle != NULL)
		return (0);
	return (__os_calloc(env, 1,
	     sizeof(*env->backup_handle), &env->backup_handle));
}

/*
 * __env_get_backup_config --
 *
 * PUBLIC: int __env_get_backup_config __P((DB_ENV *,
 * PUBLIC:      DB_BACKUP_CONFIG, u_int32_t*));
 */
int
__env_get_backup_config(dbenv, config, valuep)
	DB_ENV *dbenv;
	DB_BACKUP_CONFIG config;
	u_int32_t *valuep;
{
	DB_BACKUP	*backup;

	backup = dbenv->env->backup_handle;
	if (backup == NULL)
		return (EINVAL);

	switch (config) {
	case DB_BACKUP_WRITE_DIRECT:
		*valuep = F_ISSET(backup, BACKUP_WRITE_DIRECT);
		break;

	case DB_BACKUP_READ_COUNT:
		*valuep = backup->read_count;
		break;

	case DB_BACKUP_READ_SLEEP:
		*valuep = backup->read_sleep;
		break;

	case DB_BACKUP_SIZE:
		*valuep = backup->size;
		break;
	}
	return (0);
}

/*
 * __env_set_backup_config --
 *
 * PUBLIC: int __env_set_backup_config __P((DB_ENV *,
 * PUBLIC:      DB_BACKUP_CONFIG, u_int32_t));
 */
int
__env_set_backup_config(dbenv, config, value)
	DB_ENV *dbenv;
	DB_BACKUP_CONFIG config;
	u_int32_t value;
{
	DB_BACKUP	*backup;
	int ret;

	if ((ret = __env_backup_alloc(dbenv)) != 0)
		return (ret);

	backup = dbenv->env->backup_handle;
	switch (config) {
	case DB_BACKUP_WRITE_DIRECT:
		if (value == 0)
			F_CLR(backup, BACKUP_WRITE_DIRECT);
		else
			F_SET(backup, BACKUP_WRITE_DIRECT);
		break;

	case DB_BACKUP_READ_COUNT:
		backup->read_count = value;
		break;

	case DB_BACKUP_READ_SLEEP:
		backup->read_sleep = value;
		break;

	case DB_BACKUP_SIZE:
		backup->size = value;
		break;
	}

	return (0);
}

/*
 * __env_get_backup_callbacks --
 *
 * PUBLIC: int __env_get_backup_callbacks __P((DB_ENV *,
 * PUBLIC:     int (**)(DB_ENV *, const char *, const char *, void **),
 * PUBLIC:     int (**)(DB_ENV *,
 * PUBLIC:	    u_int32_t, u_int32_t, u_int32_t, u_int8_t *, void *),
 * PUBLIC:     int (**)(DB_ENV *, const char *, void *)));
 */
int
__env_get_backup_callbacks(dbenv, openp, writep, closep)
	DB_ENV *dbenv;
	int (**openp)(DB_ENV *, const char *, const char *, void **);
	int (**writep)(DB_ENV *,
		    u_int32_t, u_int32_t, u_int32_t, u_int8_t *, void *);
	int (**closep)(DB_ENV *, const char *, void *);
{
	DB_BACKUP	*backup;

	backup = dbenv->env->backup_handle;
	if (backup == NULL)
		return (EINVAL);

	*openp = backup->open;
	*writep = backup->write;
	*closep = backup->close;
	return (0);
}

/*
 * __env_set_backup_callbacks --
 *
 * PUBLIC: int __env_set_backup_callbacks __P((DB_ENV *,
 * PUBLIC: int (*)(DB_ENV *, const char *, const char *, void **),
 * PUBLIC: int (*)(DB_ENV *,
 * PUBLIC:     u_int32_t, u_int32_t, u_int32_t, u_int8_t *, void *),
 * PUBLIC: int (*)(DB_ENV *, const char *, void *)));
 */
int
__env_set_backup_callbacks(dbenv, open_func, write_func, close_func)
	DB_ENV *dbenv;
	int (*open_func)(DB_ENV *, const char *, const char *, void **);
	int (*write_func)(DB_ENV *,
	    u_int32_t, u_int32_t, u_int32_t, u_int8_t *, void *);
	int (*close_func)(DB_ENV *, const char *, void *);
{
	DB_BACKUP	*backup;
	int ret;

	if ((ret = __env_backup_alloc(dbenv)) != 0)
		return (ret);

	backup = dbenv->env->backup_handle;
	backup->open = open_func;
	backup->write = write_func;
	backup->close = close_func;
	return (0);
}
