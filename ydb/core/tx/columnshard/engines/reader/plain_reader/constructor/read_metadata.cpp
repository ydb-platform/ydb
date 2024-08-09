#include "read_metadata.h"
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<TScanIteratorBase> TReadMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TColumnShardScanIterator>(readContext, readContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>());
}

TConclusionStatus TReadMetadata::Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor) {
    SetPKRangesFilter(readDescription.PKRangesFilter);
    InitShardingInfo(readDescription.PathId);
    TxId = readDescription.TxId;

    /// @note We could have column name changes between schema versions:
    /// Add '1:foo', Drop '1:foo', Add '2:foo'. Drop should hide '1:foo' from reads.
    /// It's expected that we have only one version on 'foo' in blob and could split them by schema {planStep:txId}.
    /// So '1:foo' would be omitted in blob records for the column in new snapshots. And '2:foo' - in old ones.
    /// It's not possible for blobs with several columns. There should be a special logic for them.
    CommittedBlobs = dataAccessor.GetCommitedBlobs(readDescription, ResultIndexSchema->GetIndexInfo().GetReplaceKey());

    SelectInfo = dataAccessor.Select(readDescription);
    StatsMode = readDescription.StatsMode;
    return TConclusionStatus::Success();
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

std::shared_ptr<IDataReader> TReadMetadata::BuildReader(const std::shared_ptr<TReadContext>& context) const {
    return std::make_shared<TPlainReadData>(context);
}

NArrow::NMerger::TSortableBatchPosition TReadMetadata::BuildSortedPosition(const NArrow::TReplaceKey& key) const {
    return NArrow::NMerger::TSortableBatchPosition(key.ToBatch(GetReplaceKey()), 0,
        GetReplaceKey()->field_names(), {}, IsDescSorted());
}

}
