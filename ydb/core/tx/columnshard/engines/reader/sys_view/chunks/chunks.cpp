#include "chunks.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

void TStatsIterator::TSubColumnHeaderFetchingTask::DoOnDataReady(
    const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    TReadActionsCollection nextRead;
    NBlobOperations::NRead::TCompositeReadBlobs blobs = ExtractBlobsData();
    FetchingLogic.OnDataFetched(blobs, nextRead);
    if (FetchingLogic.IsDone()) {
        AFL_VERIFY(nextRead.IsEmpty());
        NActors::TActorContext::AsActorContext().Send(Context->GetScanActorId(),
            std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
                std::make_shared<TSubColumnStatsApplyResult>(FetchingLogic.ExtractResults(), std::move(WaitingCountersGuard))));
    } else {
        AFL_VERIFY(!nextRead.IsEmpty());
        std::shared_ptr<TSubColumnHeaderFetchingTask> nextReadTask = std::make_shared<TSubColumnHeaderFetchingTask>(
            std::move(nextRead), std::move(FetchingLogic), Context, GetTaskCustomer(), GetExternalTaskId());
        NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
    }
}

bool TStatsIterator::TSubColumnHeaderFetchingTask::DoOnError(
    const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())("scan_actor_id", Context->GetScanActorId())(
        "status", status.GetErrorMessage())("status_code", status.GetStatus())("storage_id", storageId);
    NActors::TActorContext::AsActorContext().Send(
        Context->GetScanActorId(), std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
                                       TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
    return false;
}

TStatsIterator::TSubColumnHeaderFetchingTask::TSubColumnHeaderFetchingTask(TReadActionsCollection&& actions,
    NArrow::NAccessor::NSubColumns::THeaderFetchingLogic&& fetchingLogic, const std::shared_ptr<NReader::TReadContext> context,
    const TString& taskCustomer, const TString& externalTaskId)
    : TBase(std::move(actions), taskCustomer, externalTaskId)
    , Context(context)
    , FetchingLogic(std::move(fetchingLogic))
    , WaitingCountersGuard(context->GetCounters().GetReadTasksGuard()) {
}

void TStatsIterator::AppendStats(
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionDataAccessor& portionPtr) const {
    const TPortionInfo& portion = portionPtr.GetPortionInfo();
    auto portionSchema = ReadMetadata->GetLoadSchemaVerified(portion);
    auto it = PortionType.find(portion.GetMeta().Produced);
    if (it == PortionType.end()) {
        it = PortionType.emplace(portion.GetMeta().Produced, ::ToString(portion.GetMeta().Produced)).first;
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
            const TString details = NeedDetails ? *TValidator::CheckNotNull(Details.FindPtr(portion.RestoreBlobRange(r->GetBlobRange()))) : "";
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            NArrow::Append<arrow::StringType>(*builders[1], prodView);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
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
                                             portion.GetBlobId(r->GetBlobRange().GetBlobIdxVerified()).ToStringLegacy())
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
            NArrow::Append<arrow::StringType>(*builders[15], arrow::util::string_view(details.data(), details.size()));
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
            NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
            NArrow::Append<arrow::StringType>(*builders[1], prodView);
            NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
            NArrow::Append<arrow::UInt64Type>(*builders[3], r->GetRecordsCount());
            NArrow::Append<arrow::UInt64Type>(*builders[4], r->GetRawBytes());
            NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
            NArrow::Append<arrow::UInt64Type>(*builders[6], r->GetChunkIdx());
            NArrow::Append<arrow::StringType>(*builders[7], ReadMetadata->GetEntityName(r->GetIndexId()).value_or("undefined"));
            NArrow::Append<arrow::UInt32Type>(*builders[8], r->GetIndexId());
            if (auto bRange = r->GetBlobRangeOptional()) {
                std::string blobIdString = portion.GetBlobId(bRange->GetBlobIdxVerified()).ToStringLegacy();
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
            NArrow::Append<arrow::StringType>(*builders[15], "");
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
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(),
        IsReverse ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC, read.GetProgram(),
        index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    ui64 recordsCount = 0;
    while (granule.GetPortions().size()) {
        auto it = FetchedAccessors.find(granule.GetPortions().front()->GetPortionId());
        if (it == FetchedAccessors.end() || !IsReady(it->first)) {
            break;
        }
        recordsCount += it->second.GetRecordsVerified().size() + it->second.GetIndexesVerified().size();
        AppendStats(builders, it->second);
        granule.PopFrontPortion();
        if (NeedDetails) {
            for (const auto& r : it->second.GetRecordsVerified()) {
                AFL_VERIFY(Details.erase(it->second.GetPortionInfo().RestoreBlobRange(r.GetBlobRange())));
            }
        }
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

TConclusionStatus TStatsIterator::Start() {
    ProcessGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildProcessGuard(ReadMetadata->GetTxId(), {});
    ScopeGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildScopeGuard(ReadMetadata->GetTxId(), 1);
    const ui32 columnsCount = ReadMetadata->GetKeyYqlSchema().size();
    for (auto&& i : IndexGranules) {
        GroupGuards.emplace_back(NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildGroupGuard(ReadMetadata->GetTxId(), 1));
        for (auto&& p : i.GetPortions()) {
            std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("SYS_VIEW::CHUNKS");
            request->AddPortion(p);
            auto allocation = std::make_shared<TFetchingAccessorAllocation>(request, p->PredictMetadataMemorySize(columnsCount), Context);
            request->RegisterSubscriber(allocation);

            NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(
                ProcessGuard->GetProcessId(), ScopeGuard->GetScopeId(), GroupGuards.back()->GetGroupId(), { allocation }, std::nullopt);
        }
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
    return IsReady(IndexGranules.front().GetPortions().front()->GetPortionId());
}

bool TStatsIterator::IsReady(const ui64 portionId) const {
    const auto* accessor = FetchedAccessors.FindPtr(portionId);
    if (!accessor) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_ready_check")("result", false)("reason", "portion_not_fetched");
        return false;
    }
    if (NeedDetails) {
        for (auto&& r : accessor->GetRecordsVerified()) {
            const auto& portion = IndexGranules.front().GetPortions().front();
            if (!Details.contains(portion->RestoreBlobRange(r.GetBlobRange()))) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_ready_check")("result", false)("reason", "details_not_ready");
                return false;
            }
        }
    }
    return true;
}

TStatsIterator::TFetchingAccessorAllocation::TFetchingAccessorAllocation(
    const std::shared_ptr<TDataAccessorsRequest>& request, const ui64 mem, const std::shared_ptr<NReader::TReadContext>& context)
    : TBase(mem)
    , AccessorsManager(context->GetDataAccessorsManager())
    , Request(request)
    , WaitingCountersGuard(context->GetCounters().GetFetcherAcessorsGuard())
    , OwnerId(context->GetScanActorId())
    , Context(context) {
}

void TStatsIterator::TFetchingAccessorAllocation::DoOnAllocationImpossible(const TString& errorMessage) {
    Request = nullptr;
    Context->AbortWithError("cannot allocate memory for take accessors info: " + errorMessage);
}

const std::shared_ptr<const TAtomicCounter>& TStatsIterator::TFetchingAccessorAllocation::DoGetAbortionFlag() const {
    return Context->GetAbortionFlag();
}

void TStatsIterator::OnAccessorReady(const TPortionDataAccessor& accessor) {
    AFL_VERIFY(FetchedAccessors.emplace(accessor.GetPortionInfo().GetPortionId(), accessor).second);

    if (NeedDetails) {
        THashMap<TString, THashMap<NOlap::TBlobRange, NArrow::NAccessor::TChunkConstructionData>> subColumnChunks;
        const auto& schema = accessor.GetPortionInfo().GetSchema(Context->GetReadMetadata()->GetIndexVersions());
        for (const auto& record : accessor.GetRecordsVerified()) {
            const auto& loader = schema->GetColumnLoaderVerified(record.GetColumnId());
            const TBlobRange blobRange = accessor.GetPortionInfo().RestoreBlobRange(record.GetBlobRange());
            if (loader->GetAccessorConstructor().GetObjectVerified().GetClassName() ==
                NArrow::NAccessor::NSubColumns::TConstructor::GetClassNameStatic()) {
                const TString& storageId = accessor.GetPortionInfo().GetColumnStorageId(record.GetColumnId(), schema->GetIndexInfo());
                AFL_VERIFY(subColumnChunks[storageId].emplace(blobRange, loader->BuildAccessorContext(0)).second);
            } else {
                AFL_VERIFY(Details.emplace(blobRange, "").second);
            }
        }

        for (auto&& [storageId, chunks] : subColumnChunks) {
            NArrow::NAccessor::NSubColumns::THeaderFetchingLogic logic(std::move(chunks), Context->GetStoragesManager()->GetOperator(storageId));
            TReadActionsCollection reads;
            logic.Start(reads);
            AFL_VERIFY(!reads.IsEmpty());
            NActors::TActivationContext::AsActorContext().Register(
                new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TSubColumnHeaderFetchingTask>(
                    std::move(reads), std::move(logic), Context, "SYS_VIEW::CHUNKS", Context->GetReadMetadata()->GetScanIdentifier())));
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSysView::NChunks
