#include "stats.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

void TReadStats::PrintToLog() {
    YDB_LOG_DEBUG("",
        {"event", "statistic"},
        {"begin", BeginTimestamp},
        {"index_granules", IndexGranules},
        {"index_portions", IndexPortions},
        {"index_batches", IndexBatches},
        {"schema_columns", SchemaColumns},
        {"filter_columns", FilterColumns},
        {"additional_columns", AdditionalColumns},
        {"compacted_portions_bytes", CompactedPortionsBytes},
        {"inserted_portions_bytes", InsertedPortionsBytes},
        {"committed_portions_bytes", CommittedPortionsBytes},
        {"data_filter_bytes", DataFilterBytes},
        {"data_additional_bytes", DataAdditionalBytes},
        {"delta_bytes", CompactedPortionsBytes + InsertedPortionsBytes + CommittedPortionsBytes - DataFilterBytes - DataAdditionalBytes},
        {"selected_rows", SelectedRows});
}

}   // namespace NKikimr::NOlap::NReader
