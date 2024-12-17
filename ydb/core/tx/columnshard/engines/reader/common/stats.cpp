#include "stats.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NOlap::NReader {

void TReadStats::PrintToLog() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("event", "statistic")
        ("begin", BeginTimestamp)
        ("index_granules", IndexGranules)
        ("index_portions", IndexPortions)
        ("index_batches", IndexBatches)
        ("committed_batches", CommittedBatches)
        ("schema_columns", SchemaColumns)
        ("filter_columns", FilterColumns)
        ("additional_columns", AdditionalColumns)
        ("compacted_portions_bytes", CompactedPortionsBytes)
        ("inserted_portions_bytes", InsertedPortionsBytes)
        ("committed_portions_bytes", CommittedPortionsBytes)
        ("data_filter_bytes", DataFilterBytes)
        ("data_additional_bytes", DataAdditionalBytes)
        ("delta_bytes", CompactedPortionsBytes + InsertedPortionsBytes + CommittedPortionsBytes - DataFilterBytes - DataAdditionalBytes)
        ("selected_rows", SelectedRows)
        ;
}

}