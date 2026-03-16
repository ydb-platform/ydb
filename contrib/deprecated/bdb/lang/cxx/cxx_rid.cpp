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

DbHeapRecordId::DbHeapRecordId()
{
	DB_HEAP_RID *rid = this;
	memset(rid, 0, sizeof(DB_HEAP_RID));
}

DbHeapRecordId::DbHeapRecordId(db_pgno_t pgno_arg, db_indx_t indx_arg)
{
	DB_HEAP_RID *rid = this;
	memset(rid, 0, sizeof(DB_HEAP_RID));
	set_pgno(pgno_arg);
	set_indx(indx_arg);
}

DbHeapRecordId::~DbHeapRecordId()
{
}

DbHeapRecordId::DbHeapRecordId(const DbHeapRecordId &that)
{
	const DB_HEAP_RID *from = &that;
	memcpy((DB_HEAP_RID *)this, from, sizeof(DB_HEAP_RID));
}

DbHeapRecordId &DbHeapRecordId::operator = (const DbHeapRecordId &that)
{
	if (this != &that) {
		const DB_HEAP_RID *from = &that;
		memcpy((DB_HEAP_RID *)this, from, sizeof(DB_HEAP_RID));
	}
	return (*this);
}
