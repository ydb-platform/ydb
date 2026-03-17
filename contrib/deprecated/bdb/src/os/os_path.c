/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
/*
 * __os_concat_path --
 *	Concatenate two elements of a path.
 * PUBLIC: int __os_concat_path __P((char *,
 * PUBLIC:     size_t, const char *, const char *));
 */
int __os_concat_path(dest, destsize, path, file)
	char *dest;
	size_t destsize;
	const char *path, *file;
{
	if ((size_t)snprintf(dest, destsize,
	    "%s%c%s", path, PATH_SEPARATOR[0], file) >= destsize)
		return (EINVAL);
	return (0);
}
