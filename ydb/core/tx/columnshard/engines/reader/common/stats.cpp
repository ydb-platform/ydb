#include "stats.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

void TReadStats::PrintToLog() {
    YDB_LOG_DEBUG("",
        {"event", "statistic"},
        {"begin", BeginTimestamp},
        {"indexGranules", IndexGranules},
        {"indexPortions", IndexPortions},
        {"indexBatches", IndexBatches},
        {"schemaColumns", SchemaColumns},
        {"filterColumns", FilterColumns},
        {"additionalColumns", AdditionalColumns},
        {"compactedPortionsBytes", CompactedPortionsBytes},
        {"insertedPortionsBytes", InsertedPortionsBytes},
        {"committedPortionsBytes", CommittedPortionsBytes},
        {"dataFilterBytes", DataFilterBytes},
        {"dataAdditionalBytes", DataAdditionalBytes},
        {"deltaBytes", CompactedPortionsBytes + InsertedPortionsBytes + CommittedPortionsBytes - DataFilterBytes - DataAdditionalBytes},
        {"selectedRows", SelectedRows});
}

}   // namespace NKikimr::NOlap::NReader
