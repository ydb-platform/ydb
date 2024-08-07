#include "constructor.h"
#include "fetched_data.h"
#include "interval.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

void IDataSource::InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching) {
    AFL_VERIFY(fetching);
    AFL_VERIFY(!FetchingPlan);
    FetchingPlan = fetching;
}

void IDataSource::RegisterInterval(TFetchingInterval& interval, const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(FetchingPlan);
    if (!IsReadyFlag) {
        AFL_VERIFY(Intervals.emplace(interval.GetIntervalIdx(), &interval).second);
    }
    if (AtomicCas(&SourceStartedFlag, 1, 0)) {
        SetFirstIntervalId(interval.GetIntervalId());
        AFL_VERIFY(FetchingPlan);
        StageData = std::make_unique<TFetchedData>(GetExclusiveIntervalOnly() && IsSourceInMemory());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", FetchingPlan->DebugString())("source_idx", SourceIdx);
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
        if (IsAborted()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "InitFetchingPlanAborted");
            return;
        }
        TFetchingScriptCursor cursor(FetchingPlan, 0);
        auto task = std::make_shared<TStepAction>(sourcePtr, std::move(cursor), Context->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }
}

void IDataSource::SetIsReady() {
    AFL_VERIFY(!IsReadyFlag);
    IsReadyFlag = true;
    for (auto&& i : Intervals) {
        i.second->OnSourceFetchStageReady(SourceIdx);
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "source_ready")("intervals_count", Intervals.size())("source_idx", SourceIdx);
    Intervals.clear();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
    THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo>& defaultBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter) {
    const NArrow::TColumnFilter& cFilter = filter ? *filter : NArrow::TColumnFilter::BuildAllowFilter();
    ui32 fetchedChunks = 0;
    ui32 nullChunks = 0;
    for (auto&& i : columnIds) {
        auto columnChunks = Portion->GetColumnChunksPointers(i);
        if (columnChunks.empty()) {
            continue;
        }
        auto itFilter = cFilter.GetIterator(false, Portion->NumRows(i));
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            Y_ABORT_UNLESS(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetNumRows())) {
                auto reading =
                    blobsAction.GetReading(Schema->GetIndexInfo().GetColumnStorageId(c->GetColumnId(), Portion->GetMeta().GetTierName()));
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Portion->RestoreBlobRange(c->BlobRange));
                ++fetchedChunks;
            } else {
                defaultBlocks.emplace(c->GetAddress(),
                    TPortionInfo::TAssembleBlobInfo(c->GetMeta().GetNumRows(), Schema->GetExternalDefaultValueVerified(c->GetColumnId())));
                ++nullChunks;
            }
            itFinished = !itFilter.Next(c->GetMeta().GetNumRows());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->NumRows(i));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "chunks_stats")("fetch", fetchedChunks)("null", nullChunks)(
        "reading_actions", blobsAction.GetStorageIds())("columns", columnIds.size());
}

bool TPortionDataSource::DoStartFetchingColumns(
    const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TColumnsSet>& columns) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName());
    AFL_VERIFY(columns->GetColumnsCount());
    AFL_VERIFY(!StageData->GetAppliedFilter() || !StageData->GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns->GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, StageData->GetAppliedFilter());
        StageData->AddDefaults(std::move(nullBlocks));
    }

    auto readActions = action.GetReadingActions();
    if (!readActions.size()) {
        return false;
    }

    auto constructor = std::make_shared<TBlobsFetcherTask>(readActions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

bool TPortionDataSource::DoStartFetchingIndexes(
    const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TIndexesSet>& indexes) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName());
    Y_ABORT_UNLESS(indexes->GetIndexesCount());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        std::set<ui32> indexIds;
        for (auto&& i : Portion->GetIndexes()) {
            if (!indexes->GetIndexIdsSet().contains(i.GetIndexId())) {
                continue;
            }
            indexIds.emplace(i.GetIndexId());
            if (auto bRange = i.GetBlobRangeOptional()) {
                auto readAction = action.GetReading(Schema->GetIndexInfo().GetIndexStorageId(i.GetIndexId()));
                readAction->SetIsBackgroundProcess(false);
                readAction->AddRange(Portion->RestoreBlobRange(*bRange));
            }
        }
        if (indexes->GetIndexIdsSet().size() != indexIds.size()) {
            return false;
        }
    }
    auto readingActions = action.GetReadingActions();
    if (!readingActions.size()) {
        NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed({});
        return false;
    }

    auto constructor = std::make_shared<TBlobsFetcherTask>(readingActions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TPortionDataSource::DoAbort() {
}

void TPortionDataSource::DoApplyIndex(const NIndexes::TIndexCheckerContainer& indexChecker) {
    THashMap<ui32, std::vector<TString>> indexBlobs;
    std::set<ui32> indexIds = indexChecker->GetIndexIds();
    //    NActors::TLogContextGuard gLog = NActors::TLogContextBuilder::Build()("records_count", GetRecordsCount())("portion_id", Portion->GetAddress().DebugString());
    std::vector<TPortionInfo::TPage> pages = Portion->BuildPages();
    NArrow::TColumnFilter constructor = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& p : pages) {
        for (auto&& i : p.GetIndexes()) {
            if (!indexIds.contains(i->GetIndexId())) {
                continue;
            }
            if (i->HasBlobData()) {
                indexBlobs[i->GetIndexId()].emplace_back(i->GetBlobDataVerified());
            } else {
                indexBlobs[i->GetIndexId()].emplace_back(StageData->ExtractBlob(i->GetAddress()));
            }
        }
        for (auto&& i : indexIds) {
            if (!indexBlobs.contains(i)) {
                return;
            }
        }
        if (indexChecker->Check(indexBlobs)) {
            NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed(true);
            constructor.Add(true, p.GetRecordsCount());
        } else {
            NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed(false);
            constructor.Add(false, p.GetRecordsCount());
        }
    }
    AFL_VERIFY(constructor.Size() == Portion->GetRecordsCount());
    if (constructor.IsTotalDenyFilter()) {
        StageData->AddFilter(NArrow::TColumnFilter::BuildDenyFilter());
    } else if (constructor.IsTotalAllowFilter()) {
        return;
    } else {
        StageData->AddFilter(constructor);
    }
}

void TPortionDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) {
    auto blobSchema = GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*Portion);
    MutableStageData().AddBatch(Portion->PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs())
                                    .AssembleToGeneralContainer(SequentialEntityIds));
}

bool TCommittedDataSource::DoStartFetchingColumns(
    const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TColumnsSet>& /*columns*/) {
    if (ReadStarted) {
        return false;
    }
    ReadStarted = true;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<IBlobsStorageOperator> storageOperator = GetContext()->GetCommonContext()->GetStoragesManager()->GetInsertOperator();
    auto readAction = storageOperator->StartReadingAction(NBlobOperations::EConsumer::SCAN);

    readAction->SetIsBackgroundProcess(false);
    readAction->AddRange(CommittedBlob.GetBlobRange());

    std::vector<std::shared_ptr<IBlobsReadingAction>> actions = { readAction };
    auto constructor = std::make_shared<TBlobsFetcherTask>(actions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TCommittedDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) {
    TMemoryProfileGuard mGuard("SCAN_PROFILE::ASSEMBLER::COMMITTED", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    if (!GetStageData().GetTable()) {
        AFL_VERIFY(GetStageData().GetBlobs().size() == 1);
        auto bData = MutableStageData().ExtractBlob(GetStageData().GetBlobs().begin()->first);
        auto schema = GetContext()->GetReadMetadata()->GetBlobSchema(CommittedBlob.GetSchemaVersion());
        auto rBatch = NArrow::DeserializeBatch(bData, std::make_shared<arrow::Schema>(CommittedBlob.GetSchemaSubset().Apply(schema->fields())));
        AFL_VERIFY(rBatch)("schema", schema->ToString());
        auto batch = std::make_shared<NArrow::TGeneralContainer>(rBatch);
        GetContext()->GetReadMetadata()->GetIndexInfo().AddSnapshotColumns(*batch, CommittedBlob.GetSnapshot());
        GetContext()->GetReadMetadata()->GetIndexInfo().AddDeleteFlagsColumn(*batch, CommittedBlob.GetIsDelete());
        MutableStageData().AddBatch(batch);
    }
    MutableStageData().SyncTableColumns(columns->GetSchema()->fields(), *GetContext()->GetReadMetadata()->GetResultSchema());
}

}   // namespace NKikimr::NOlap::NReader::NPlain
