#include "read_metadata.h"
#include "read_context.h"
#include "plain_reader/plain_read_data.h"
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard__stats_scan.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

TDataStorageAccessor::TDataStorageAccessor(const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                const std::unique_ptr<NOlap::IColumnEngine>& index)
    : InsertTable(insertTable)
    , Index(index)
{}

std::shared_ptr<NOlap::TSelectInfo> TDataStorageAccessor::Select(const NOlap::TReadDescription& readDescription) const {
    if (readDescription.ReadNothing) {
        return std::make_shared<NOlap::TSelectInfo>();
    }
    return Index->Select(readDescription.PathId,
                            readDescription.GetSnapshot(),
                            readDescription.PKRangesFilter);
}

std::vector<NOlap::TCommittedBlob> TDataStorageAccessor::GetCommitedBlobs(const NOlap::TReadDescription& readDescription, const std::shared_ptr<arrow::Schema>& pkSchema) const {
    return std::move(InsertTable->Read(readDescription.PathId, readDescription.GetSnapshot(), pkSchema));
}

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadMetadata::StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const {
    return std::make_unique<NColumnShard::TColumnShardScanIterator>(readContext, this->shared_from_this());
}

bool TReadMetadata::Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor, std::string& /*error*/) {
    SetPKRangesFilter(readDescription.PKRangesFilter);

    /// @note We could have column name changes between schema versions:
    /// Add '1:foo', Drop '1:foo', Add '2:foo'. Drop should hide '1:foo' from reads.
    /// It's expected that we have only one version on 'foo' in blob and could split them by schema {planStep:txId}.
    /// So '1:foo' would be omitted in blob records for the column in new snapshots. And '2:foo' - in old ones.
    /// It's not possible for blobs with several columns. There should be a special logic for them.
    CommittedBlobs = dataAccessor.GetCommitedBlobs(readDescription, ResultIndexSchema->GetIndexInfo().GetReplaceKey());

    SelectInfo = dataAccessor.Select(readDescription);
    StatsMode = readDescription.StatsMode;
    return true;
}

std::set<ui32> TReadMetadata::GetEarlyFilterColumnIds() const {
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    std::set<ui32> result;
    for (auto&& i : GetProgram().GetEarlyFilterColumns()) {
        auto id = indexInfo.GetColumnIdOptional(i);
        if (id) {
            result.emplace(*id);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i);
        }
    }
    return result;
}

std::set<ui32> TReadMetadata::GetPKColumnIds() const {
    std::set<ui32> result;
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    for (auto&& i : indexInfo.GetPrimaryKeyColumns()) {
        Y_ABORT_UNLESS(result.emplace(indexInfo.GetColumnId(i.first)).second);
    }
    return result;
}

std::vector<std::pair<TString, NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return NOlap::GetColumns(NColumnShard::PrimaryIndexStatsSchema, NColumnShard::PrimaryIndexStatsSchema.KeyColumns);
}

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<NOlap::TReadContext>& /*readContext*/) const {
    return std::make_unique<NColumnShard::TStatsIterator>(this->shared_from_this());
}

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

std::shared_ptr<NKikimr::NOlap::IDataReader> TReadMetadata::BuildReader(const std::shared_ptr<NOlap::TReadContext>& context) const {
    return std::make_shared<NPlainReader::TPlainReadData>(context);
//    auto result = std::make_shared<TIndexedReadData>(self, context);
//    result->InitRead();
//    return result;
}

NIndexedReader::TSortableBatchPosition TReadMetadata::BuildSortedPosition(const NArrow::TReplaceKey& key) const {
    return NIndexedReader::TSortableBatchPosition(key.ToBatch(GetReplaceKey()), 0,
        GetReplaceKey()->field_names(), {}, IsDescSorted());
}

}
