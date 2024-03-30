#include "source.h"
#include "interval.h"
#include "fetched_data.h"
#include "plain_read_data.h"
#include "constructor.h"
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NPlain {

void IDataSource::InitFetchingPlan(const std::shared_ptr<IFetchingStep>& fetchingFirstStep, const std::shared_ptr<IDataSource>& sourcePtr, const bool isExclusive) {
    AFL_VERIFY(fetchingFirstStep);
    if (AtomicCas(&FilterStageFlag, 1, 0)) {
        StageData = std::make_unique<TFetchedData>(isExclusive);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", fetchingFirstStep->DebugString())("source_idx", SourceIdx);
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
        if (IsAborted()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "InitFetchingPlanAborted");
            return;
        }
        if (fetchingFirstStep->ExecuteInplace(sourcePtr, fetchingFirstStep)) {
            auto task = std::make_shared<TStepAction>(sourcePtr, fetchingFirstStep->GetNextStep(), Context->GetCommonContext()->GetScanActorId());
            NConveyor::TScanServiceOperator::SendTaskToExecute(task);
        }
    }
}

void IDataSource::RegisterInterval(TFetchingInterval& interval) {
    if (!IsReadyFlag) {
        AFL_VERIFY(Intervals.emplace(interval.GetIntervalIdx(), &interval).second);
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

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds,
    TBlobsAction& blobsAction, THashMap<TChunkAddress, ui32>& nullBlocks,
    const std::shared_ptr<NArrow::TColumnFilter>& filter) {
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
                auto reading = blobsAction.GetReading(Schema->GetIndexInfo().GetColumnStorageId(c->GetColumnId(), Portion->GetMeta().GetTierName()));
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Portion->RestoreBlobRange(c->BlobRange));
                ++fetchedChunks;
            } else {
                nullBlocks.emplace(c->GetAddress(), c->GetMeta().GetNumRows());
                ++nullChunks;
            }
            itFinished = !itFilter.Next(c->GetMeta().GetNumRows());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->NumRows(i));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "chunks_stats")("fetch", fetchedChunks)("null", nullChunks)("reading_actions", blobsAction.GetStorageIds())("columns", columnIds.size());
}

bool TPortionDataSource::DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName());
    AFL_VERIFY(columns->GetColumnsCount());
    AFL_VERIFY(!StageData->GetAppliedFilter() || !StageData->GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns->GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName())("fetching_info", step->DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), "CS::READ::" + step->GetName());
    {
        THashMap<TChunkAddress, ui32> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, StageData->GetAppliedFilter());
        StageData->AddNulls(std::move(nullBlocks));
    }

    auto readActions = action.GetReadingActions();
    if (!readActions.size()) {
        return false;
    }

    auto constructor = std::make_shared<TBlobsFetcherTask>(readActions, sourcePtr, step, GetContext(), "CS::READ::" + step->GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

bool TPortionDataSource::DoStartFetchingIndexes(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TIndexesSet>& indexes) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName());
    Y_ABORT_UNLESS(indexes->GetIndexesCount());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName())("fetching_info", step->DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), "CS::READ::" + step->GetName());
    {
        std::set<ui32> indexIds;
        for (auto&& i : Portion->GetIndexes()) {
            if (!indexes->GetIndexIdsSet().contains(i.GetIndexId())) {
                continue;
            }
            indexIds.emplace(i.GetIndexId());
            auto readAction = action.GetReading(Schema->GetIndexInfo().GetIndexStorageId(i.GetIndexId()));
            readAction->SetIsBackgroundProcess(false);
            readAction->AddRange(Portion->RestoreBlobRange(i.GetBlobRange()));
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

    auto constructor = std::make_shared<TBlobsFetcherTask>(readingActions, sourcePtr, step, GetContext(), "CS::READ::" + step->GetName(), "");
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
            indexBlobs[i->GetIndexId()].emplace_back(StageData->ExtractBlob(i->GetAddress()));
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

bool TCommittedDataSource::DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& /*columns*/) {
    if (ReadStarted) {
        return false;
    }
    ReadStarted = true;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName())("fetching_info", step->DebugString());

    std::shared_ptr<IBlobsStorageOperator> storageOperator = GetContext()->GetCommonContext()->GetStoragesManager()->GetInsertOperator();
    auto readAction = storageOperator->StartReadingAction("CS::READ::" + step->GetName());

    readAction->SetIsBackgroundProcess(false);
    readAction->AddRange(CommittedBlob.GetBlobRange());

    std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
    auto constructor = std::make_shared<TBlobsFetcherTask>(actions, sourcePtr, step, GetContext(), "CS::READ::" + step->GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TCommittedDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) {
    if (!GetStageData().GetTable()) {
        Y_ABORT_UNLESS(GetStageData().GetBlobs().size() == 1);
        auto bData = MutableStageData().ExtractBlob(GetStageData().GetBlobs().begin()->first);
        auto batch = NArrow::DeserializeBatch(bData, GetContext()->GetReadMetadata()->GetBlobSchema(CommittedBlob.GetSchemaVersion()));
        Y_ABORT_UNLESS(batch);
        batch = GetContext()->GetReadMetadata()->GetIndexInfo().AddSpecialColumns(batch, CommittedBlob.GetSnapshot());
        MutableStageData().AddBatch(batch);
    }
    MutableStageData().SyncTableColumns(columns->GetSchema()->fields());
}

}
