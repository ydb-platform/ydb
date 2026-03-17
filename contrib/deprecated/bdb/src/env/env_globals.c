/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

/*
 * A structure with static initialization values for all of the global fields
 * used by Berkeley DB.
 * See dbinc/globals.h for the structure definition.
 */
DB_GLOBALS __db_global_values = {
#ifdef HAVE_VXWORKS
	0,				/* VxWorks: db_global_init */
	NULL,				/* VxWorks: db_global_lock */
#endif
#ifdef DB_WIN32
#ifndef DB_WINCE
	{ 0 },			/* SECURITY_DESCRIPTOR win_default_sec_desc */
	{ 0 },			/* SECURITY_ATTRIBUTES win_default_sec_attr */
#endif
	NULL,				/* SECURITY_ATTRIBUTES *win_sec_attr */
#endif
	{ NULL, NULL },			/* XA env list */

	"=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=", /* db_line */
	{ 0 },				/* error_buf */
	0,				/* uid_init */
	0,				/* rand_next */
	0,				/* fid_serial */
	0,				/* db_errno */
	0,				/* num_active_pids */
	0,				/* size_active_pids */
	NULL,                           /* active_pids */
	NULL,                           /* saved_errstr */
	NULL,				/* j_assert */
	NULL,				/* j_close */
	NULL,				/* j_dirfree */
	NULL,				/* j_dirlist */
	NULL,				/* j_exists*/
	NULL,				/* j_free */
	NULL,				/* j_fsync */
	NULL,				/* j_ftruncate */
	NULL,				/* j_ioinfo */
	NULL,				/* j_malloc */
	NULL,				/* j_file_map */
	NULL,				/* j_file_unmap */
	NULL,				/* j_open */
	NULL,				/* j_pread */
	NULL,				/* j_pwrite */
	NULL,				/* j_read */
	NULL,				/* j_realloc */
	NULL,				/* j_region_map */
	NULL,				/* j_region_unmap */
	NULL,				/* j_rename */
	NULL,				/* j_seek */
	NULL,				/* j_unlink */
	NULL,				/* j_write */
	NULL				/* j_yield */
};
