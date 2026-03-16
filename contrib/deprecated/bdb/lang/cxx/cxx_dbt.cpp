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

#include <contrib/deprecated/bdb/src/dbinc/db_page.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/db_auto.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/crdel_auto.h>
#include <contrib/deprecated/bdb/src/dbinc/db_dispatch.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/db_ext.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/common_ext.h>

Dbt::Dbt()
{
	DBT *dbt = this;
	memset(dbt, 0, sizeof(DBT));
}

Dbt::Dbt(void *data_arg, u_int32_t size_arg)
{
	DBT *dbt = this;
	memset(dbt, 0, sizeof(DBT));
	set_data(data_arg);
	set_size(size_arg);
}

Dbt::~Dbt()
{
}

Dbt::Dbt(const Dbt &that)
{
	const DBT *from = &that;
	DBT *to = this;
	memcpy(to, from, sizeof(DBT));
}

Dbt &Dbt::operator = (const Dbt &that)
{
	if (this != &that) {
		const DBT *from = &that;
		DBT *to = this;
		memcpy(to, from, sizeof(DBT));
	}
	return (*this);
}
