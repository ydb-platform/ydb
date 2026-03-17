/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

#include "db_cxx.h"
#include <contrib/deprecated/bdb/src/dbinc/cxx_int.h>

// Helper macro for simple methods that pass through to the
// underlying C method. It may return an error or raise an exception.
// Note this macro expects that input _argspec is an argument
// list element (e.g., "char *arg") and that _arglist is the arguments
// that should be passed through to the C method (e.g., "(dbsite, arg)")
//
#define	DB_SITE_METHOD(_name, _delete, _argspec, _arglist, _retok)	\
int DbSite::_name _argspec						\
{									\
	int ret;							\
	DB_SITE *dbsite = unwrap(this);					\
									\
	if (dbsite == NULL)						\
		ret = EINVAL;						\
	else								\
		ret = dbsite->_name _arglist;				\
	if (_delete)							\
		delete this;						\
        if (!_retok(ret))						\
                DB_ERROR(DbEnv::get_DbEnv(dbsite->env->dbenv),		\
		    "DbSite::"#_name, ret, ON_ERROR_UNKNOWN);		\
	return (ret);							\
}

DbSite::DbSite()
:       imp_(0)
{
}

DbSite::~DbSite()
{
}

DB_SITE_METHOD(close, 1, (), (dbsite), DB_RETOK_STD)
DB_SITE_METHOD(get_address, 0, (const char **hostp, u_int *port),
    (dbsite, hostp, port), DB_RETOK_STD)
DB_SITE_METHOD(get_config, 0, (u_int32_t which, u_int32_t *valuep),
    (dbsite, which, valuep), DB_RETOK_STD)
DB_SITE_METHOD(get_eid, 0, (int *eidp), (dbsite, eidp), DB_RETOK_STD)
DB_SITE_METHOD(remove, 1, (), (dbsite), DB_RETOK_STD)
DB_SITE_METHOD(set_config, 0, (u_int32_t which, u_int32_t value),
    (dbsite, which, value), DB_RETOK_STD)
