/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2012 Oracle and/or its affiliates.  All rights reserved.
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
// that should be passed through to the C method (e.g., "(dbchannel, arg)")
//
#define	DB_CHANNEL_METHOD(_name, _delete, _argspec, _arglist, _retok)	\
int DbChannel::_name _argspec						\
{									\
	int ret;							\
	DB_CHANNEL *dbchannel = unwrap(this);				\
									\
	if (dbchannel == NULL)						\
		ret = EINVAL;						\
	else								\
		ret = dbchannel->_name _arglist;			\
	if (_delete)							\
		delete this;						\
	if (!_retok(ret))						\
		DB_ERROR(dbenv_, "DbChannel::"#_name, ret,		\
		    ON_ERROR_UNKNOWN);					\
	return (ret);							\
}

DbChannel::DbChannel()
:       imp_(0)
{
}

DbChannel::~DbChannel()
{
}

DB_CHANNEL_METHOD(close, 1, (), (dbchannel, 0), DB_RETOK_STD)

int DbChannel::send_msg(Dbt *msg, u_int32_t nmsg, u_int32_t flags)
{
	DB_CHANNEL *dbchannel = unwrap(this);
	DB_ENV *dbenv = unwrap(dbenv_);
	DBT *dbtlist;
	int i, ret;

	ret = __os_malloc(dbenv->env, sizeof(DBT) * nmsg, &dbtlist);
	if (ret != 0) {
		DB_ERROR(dbenv_, "DbChannel::send_msg", ret, ON_ERROR_UNKNOWN);
		return (ret);
	}

	for (i = 0; i < (int)nmsg; i++)
		memcpy(&dbtlist[i], msg[i].get_DBT(), sizeof(DBT));

	if ((ret = dbchannel->send_msg(dbchannel, dbtlist, nmsg, flags)) != 0)
		DB_ERROR(dbenv_, "DbChannel::send_msg", ret, ON_ERROR_UNKNOWN);

	__os_free(dbenv->env, dbtlist);

	return (ret);
}

int DbChannel::send_request(Dbt *request, u_int32_t nrequest,
    Dbt *response, db_timeout_t timeout, u_int32_t flags)
{
	DB_CHANNEL *dbchannel = unwrap(this);
	DB_ENV *dbenv = unwrap(dbenv_);
	DBT *dbtlist;
	int i, ret;

	ret = __os_malloc(dbenv->env, sizeof(DBT) * nrequest, &dbtlist);
	if (ret != 0) {
		DB_ERROR(dbenv_, "DbChannel::send_request", ret,
		    ON_ERROR_UNKNOWN);
		return (ret);
	}

	for (i = 0; i < (int)nrequest; i++)
		memcpy(&dbtlist[i], request[i].get_DBT(), sizeof(DBT));

	if ((ret = dbchannel->send_request(dbchannel, dbtlist, nrequest,
	    response->get_DBT(), timeout, flags)) != 0)
		DB_ERROR(dbenv_, "DbChannel::send_request", ret,
		    ON_ERROR_UNKNOWN);

	__os_free(dbenv->env, dbtlist);

	return (ret);
}

DB_CHANNEL_METHOD(set_timeout, 0, (db_timeout_t timeout),
    (dbchannel, timeout), DB_RETOK_STD);
