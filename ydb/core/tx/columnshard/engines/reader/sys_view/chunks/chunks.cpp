#include "chunks.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/data_reader/contexts.h>
#include <ydb/core/tx/columnshard/data_reader/fetcher.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
    const NColumnShard::TSchemeShardLocalPathId schemshardLocalPathId, const TPortionDataAccessor& portionPtr) const {
    const TPortionInfo& portion = portionPtr.GetPortionInfo();
    auto portionSchema = ReadMetadata->GetLoadSchemaVerified(portion);
    auto it = PortionType.find(portion.GetProduced());
    if (it == PortionType.end()) {
        it = PortionType.emplace(portion.GetProduced(), ::ToString(portion.GetProduced())).first;
    }
    const arrow::util::string_view prodView = it->second.GetView();
    const bool activity = !portion.HasRemoveSnapshot();
    static const TString ConstantEntityIsColumn = "COL";
    static const arrow::util::string_view ConstantEntityIsColumnView =
        arrow::util::string_view(ConstantEntityIsColumn.data(), ConstantEntityIsColumn.size());
    static const TString ConstantEntityIsIndex = "IDX";
    static const arrow::util::string_view ConstantEntityIsIndexView =
        arrow::util::string_view(ConstantEntityIsIndex.data(), ConstantEntityIsIndex.size());
    auto& entityStorages = EntityStorageNames[portion.GetMeta().GetTierName()];
    {
        std::vector<const TColumnRecord*> records;
        for (auto&& r : portionPtr.GetRecordsVerified()) {
            records.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(records.begin(), records.end());
        }
        THashMap<ui32, TString> blobsIds;
        std::optional<ui32> lastColumnId;
        arrow::util::string_view lastColumnName;
        arrow::util::string_view lastTierName;
        for (auto&& r : records) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], schemshardLocalPathId.GetRawValue());
            NArrow::Append<arrow::StringType>(*builders[1], prodView);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->GetTabletId());
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetMeta().GetRecordsCount());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetMeta().GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            if (!lastColumnId || *lastColumnId != r->GetColumnId()) {
                {
                    auto it = ColumnNamesById.find(r->GetColumnId());
                    if (it == ColumnNamesById.end()) {
                        it =
                            ColumnNamesById.emplace(r->GetColumnId(), portionSchema->GetFieldByColumnIdVerified(r->GetColumnId())->name()).first;
                    }
                    lastColumnName = it->second.GetView();
                }
                {
                    auto it = entityStorages.find(r->GetColumnId());
                    if (it == entityStorages.end()) {
                        it =
                            entityStorages.emplace(r->GetColumnId(), portion.GetEntityStorageId(r->GetColumnId(), portionSchema->GetIndexInfo()))
                                .first;
                    }
                    lastTierName = it->second.GetView();
                }
                lastColumnId = r->GetColumnId();
            }
            NArrow::Append<arrow::StringType>(*builders[7], lastColumnName);
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetColumnId());
            {
                auto itBlobIdString = blobsIds.find(r->GetBlobRange().GetBlobIdxVerified());
                if (itBlobIdString == blobsIds.end()) {
                    itBlobIdString = blobsIds
                                         .emplace(r->GetBlobRange().GetBlobIdxVerified(),
                                             portionPtr.GetBlobId(r->GetBlobRange().GetBlobIdxVerified()).ToStringLegacy())
                                         .first;
                }
                NArrow::Append<arrow::StringType>(
                    *builders[9], arrow::util::string_view(itBlobIdString->second.data(), itBlobIdString->second.size()));
            }
            NArrow::Append<arrow::UInt64Type>(*builders[10], r->BlobRange.Offset);
            NArrow::Append<arrow::UInt64Type>(*builders[11], r->BlobRange.Size);
            NArrow::Append<arrow::UInt8Type>(*builders[12], activity);

            NArrow::Append<arrow::StringType>(*builders[13], arrow::util::string_view(lastTierName.data(), lastTierName.size()));
            NArrow::Append<arrow::StringType>(*builders[14], ConstantEntityIsColumnView);
        }
    }
    {
        std::vector<const TIndexChunk*> indexes;
        for (auto&& r : portionPtr.GetIndexesVerified()) {
            indexes.emplace_back(&r);
        }
        if (Reverse) {
            std::reverse(indexes.begin(), indexes.end());
        }
        for (auto&& r : indexes) {
            NArrow::Append<arrow::UInt64Type>(*builders[0], schemshardLocalPathId.GetRawValue());
            NArrow::Append<arrow::StringType>(*builders[1], prodView);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->GetTabletId());
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetRecordsCount());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetEntityName(r->GetIndexId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetIndexId());
            if (auto bRange = r->GetBlobRangeOptional()) {
                std::string blobIdString = portionPtr.GetBlobId(bRange->GetBlobIdxVerified()).ToStringLegacy();
                NArrow::Append<arrow::StringType>(*builders[9], blobIdString);
                NArrow::Append<arrow::UInt64Type>(*builders[10], bRange->Offset);
                NArrow::Append<arrow::UInt64Type>(*builders[11], bRange->Size);
            } else if (auto bData = r->GetBlobDataOptional()) {
                NArrow::Append<arrow::StringType>(*builders[9], "INPLACE");
                NArrow::Append<arrow::UInt64Type>(*builders[10], 0);
                NArrow::Append<arrow::UInt64Type>(*builders[11], bData->size());
            }
            NArrow::Append<arrow::UInt8Type>(*builders[12], activity);
            const auto tierName = portion.GetEntityStorageId(r->GetIndexId(), portionSchema->GetIndexInfo());
            std::string strTierName(tierName.data(), tierName.size());
            NArrow::Append<arrow::StringType>(*builders[13], strTierName);
            NArrow::Append<arrow::StringType>(*builders[14], ConstantEntityIsIndexView);
        }
    }
}

std::unique_ptr<TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TStatsIterator>(readContext);
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return GetColumns(TStatsIterator::StatsSchema, TStatsIterator::StatsSchema.KeyColumns);
}

std::shared_ptr<NAbstract::TReadStatsMetadata> TConstructor::BuildMetadata(
    const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto* index = self->GetIndexOptional();
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(), Sorting, read.GetProgram(),
        index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    ui64 recordsCount = 0;
    while (granule.GetPortions().size()) {
        auto it = FetchedAccessors.find(granule.GetPortions().front()->GetPortionId());
        if (it == FetchedAccessors.end()) {
            break;
        }
        recordsCount += it->second.GetRecordsVerified().size() + it->second.GetIndexesVerified().size();
        AppendStats(builders, granule.GetPathId().SchemeShardLocalPathId, it->second);
        granule.PopFrontPortion();
        FetchedAccessors.erase(it);
        if (recordsCount > 10000) {
            break;
        }
    }
    return granule.GetPortions().size();
}

ui32 TStatsIterator::PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const {
    ui32 recordsCount = 0;
    for (auto&& portion : granule.GetPortions()) {
        auto it = FetchedAccessors.find(portion->GetPortionId());
        if (it == FetchedAccessors.end()) {
            break;
        }
        recordsCount += it->second.GetRecordsVerified().size() + it->second.GetIndexesVerified().size();
        if (recordsCount > 10000) {
            break;
        }
    }
    AFL_VERIFY(recordsCount || granule.GetPortions().empty());
    return recordsCount;
}

namespace {

class TApplyResult: public IApplyAction {
private:
    using TBase = IDataTasksProcessor::ITask;
    YDB_READONLY_DEF(std::vector<TPortionDataAccessor>, Accessors);

public:
    TApplyResult(const std::vector<TPortionDataAccessor>& accessors)
        : Accessors(accessors) {
    }

    virtual bool DoApply(IDataReader& /*indexedDataRead*/) const override {
        AFL_VERIFY(false);
        return false;
    }
};

class TFetchingExecutor: public NOlap::NDataFetcher::IFetchCallback {
private:
    const NActors::TActorId ParentActorId;
    NColumnShard::TCounterGuard WaitingCountersGuard;
    std::shared_ptr<const TAtomicCounter> AbortionFlag;

    virtual bool IsAborted() const override {
        return AbortionFlag->Val();
    }

    virtual TString GetClassName() const override {
        return "SYS_VIEW";
    }

    virtual void DoOnFinished(NOlap::NDataFetcher::TCurrentContext&& context) override {
        NActors::TActivationContext::AsActorContext().Send(
            ParentActorId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
                               std::make_shared<TApplyResult>(context.ExtractPortionAccessors()), std::move(WaitingCountersGuard)));
    }
    virtual void DoOnError(const TString& errorMessage) override {
        NActors::TActivationContext::AsActorContext().Send(
            ParentActorId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
                               TConclusionStatus::Fail("cannot fetch accessors: " + errorMessage), std::move(WaitingCountersGuard)));
    }

public:
    TFetchingExecutor(const std::shared_ptr<TReadContext>& context)
        : ParentActorId(context->GetScanActorId())
        , WaitingCountersGuard(context->GetCounters().GetFetcherAcessorsGuard())
        , AbortionFlag(context->GetAbortionFlag())
    {
    }
};

}   // namespace

TConclusionStatus TStatsIterator::Start() {
    std::vector<TPortionInfo::TConstPtr> portions;
    std::shared_ptr<TVersionedIndex> actualIndexInfo;
    auto env = std::make_shared<NOlap::NDataFetcher::TEnvironment>(Context->GetDataAccessorsManager(), Context->GetStoragesManager());
    for (auto&& i : IndexGranules) {
        for (auto&& p : i.GetPortions()) {
            portions.emplace_back(p);
            if (portions.size() == 100) {
                if (!actualIndexInfo) {
                    actualIndexInfo = ReadMetadata->GetIndexVersionsPtr();
                }
                NOlap::NDataFetcher::TRequestInput rInput(
                    std::move(portions), actualIndexInfo, NOlap::NBlobOperations::EConsumer::SYS_VIEW_SCAN, ::ToString(ReadMetadata->GetTxId()));
                NOlap::NDataFetcher::TPortionsDataFetcher::StartAccessorPortionsFetching(
                    std::move(rInput), std::make_shared<TFetchingExecutor>(Context), env, NConveyorComposite::ESpecialTaskCategory::Scan);
                portions.clear();
            }
        }
    }
    if (portions.size()) {
        if (!actualIndexInfo) {
            actualIndexInfo = ReadMetadata->GetIndexVersionsPtr();
        }
        NOlap::NDataFetcher::TRequestInput rInput(
            std::move(portions), actualIndexInfo, NOlap::NBlobOperations::EConsumer::SYS_VIEW_SCAN, ::ToString(ReadMetadata->GetTxId()));
        NOlap::NDataFetcher::TPortionsDataFetcher::StartAccessorPortionsFetching(
            std::move(rInput), std::make_shared<TFetchingExecutor>(Context), env, NConveyorComposite::ESpecialTaskCategory::Scan);
    }
    return TConclusionStatus::Success();
}

bool TStatsIterator::IsReadyForBatch() const {
    if (!IndexGranules.size()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_ready_check")("result", false)("reason", "no_granules");
        return false;
    }
    if (!IndexGranules.front().GetPortions().size()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_ready_check")("result", true)("reason", "no_granule_portions");
        return true;
    }
    if (FetchedAccessors.contains(IndexGranules.front().GetPortions().front()->GetPortionId())) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_ready_check")("result", true)("reason", "portion_fetched");
        return true;
    }
    return false;
}

void TStatsIterator::Apply(const std::shared_ptr<IApplyAction>& task) {
    if (IndexGranules.empty()) {
        return;
    }
    auto result = std::dynamic_pointer_cast<TApplyResult>(task);
    AFL_VERIFY(result);
    for (auto&& i : result->GetAccessors()) {
        FetchedAccessors.emplace(i.GetPortionInfo().GetPortionId(), i);
    }
    
}

}   // namespace NKikimr::NOlap::NReader::NSysView::NChunks
