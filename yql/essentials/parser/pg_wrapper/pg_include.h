#pragma once

#include "pg_compat.h"

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#define Sort PG_Sort
#define Unique PG_Unique
#undef SIZEOF_SIZE_T

extern "C" {

#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_conversion_d.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/typcache.h"
#include "utils/memutils_internal.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "thread_inits.h"

#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef Sort
#undef Unique
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t

constexpr auto PG_DAY = DAY;
constexpr auto PG_SECOND = SECOND;
constexpr auto PG_ERROR = ERROR;

#undef DAY
#undef SECOND
#undef ERROR
#undef INVALID

}
