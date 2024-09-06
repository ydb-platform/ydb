#pragma once

#include <ydb/library/yql/core/pg_settings/guc_settings.h>
#include <util/generic/string.h>

extern "C" {
#include "c.h"
#include "postgres.h"
#include "access/htup.h"
#include "datatype/timestamp.h"
#include "miscadmin.h"
#include "utils/palloc.h"
#include "nodes/memnodes.h"
#include "utils/typcache.h"
}

#undef TypeName
#undef Max
#undef bind

struct TMainContext {
    MemoryContextData Data;
    MemoryContextData ErrorData;
    MemoryContext PrevCurrentMemoryContext = nullptr;
    MemoryContext PrevErrorContext = nullptr;
    MemoryContext PrevCacheMemoryContext = nullptr;
    RecordCacheState CurrentRecordCacheState = { NULL, NULL, 0, 0, INVALID_TUPLEDESC_IDENTIFIER };
    RecordCacheState PrevRecordCacheState;
    TimestampTz StartTimestamp;
    pg_stack_base_t PrevStackBase;
    TString LastError;
    TGUCSettings::TPtr GUCSettings;
    HeapTuple CurrentDatabaseName = nullptr;
    HeapTuple CurrentUserName = nullptr;
};
