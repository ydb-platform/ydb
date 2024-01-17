#include "source.h"
#include "interval.h"
#include "fetched_data.h"
#include "plain_read_data.h"
#include "constructor.h"
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

void IDataSource::InitFetchingPlan(const std::shared_ptr<IFetchingStep>& fetchingFirstStep, const std::shared_ptr<IDataSource>& sourcePtr, const bool isExclusive) {
    AFL_VERIFY(fetchingFirstStep);
    if (AtomicCas(&FilterStageFlag, 1, 0)) {
        StageData = std::make_shared<TFetchedData>(isExclusive);
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
    const std::shared_ptr<IBlobsReadingAction>& readingAction, THashMap<TBlobRange, ui32>& nullBlocks,
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
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetNumRowsVerified())) {
                readingAction->AddRange(c->BlobRange);
                ++fetchedChunks;
            } else {
                nullBlocks.emplace(c->BlobRange, c->GetMeta().GetNumRowsVerified());
                ++nullChunks;
            }
            itFinished = !itFilter.Next(c->GetMeta().GetNumRowsVerified());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->NumRows(i));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "chunks_stats")("fetch", fetchedChunks)("null", nullChunks)("reading_action", readingAction->GetStorageId())("columns", columnIds.size());
}

bool TPortionDataSource::DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr,
    const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName());
    Y_ABORT_UNLESS(columns->GetColumnsCount());
    auto& columnIds = columns->GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step->GetName())("fetching_info", step->DebugString());

    auto readAction = Portion->GetBlobsStorage()->StartReadingAction("CS::READ::" + step->GetName());
    readAction->SetIsBackgroundProcess(false);
    {
        THashMap<TBlobRange, ui32> nullBlocks;
        NeedFetchColumns(columnIds, readAction, nullBlocks, StageData->GetAppliedFilter());
        StageData->AddNulls(std::move(nullBlocks));
    }

    std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
    auto constructor = std::make_shared<TBlobsFetcherTask>(actions, sourcePtr, step, GetContext(), "CS::READ::" + step->GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TPortionDataSource::DoAbort() {
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

void TCommittedDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& /*columns*/) {
    if (!!GetStageData().GetTable()) {
        return;
    }
    Y_ABORT_UNLESS(GetStageData().GetBlobs().size() == 1);
    auto bData = MutableStageData().ExtractBlob(GetStageData().GetBlobs().begin()->first);
    auto batch = NArrow::DeserializeBatch(bData, GetContext()->GetReadMetadata()->GetBlobSchema(CommittedBlob.GetSchemaVersion()));
    Y_ABORT_UNLESS(batch);
    batch = GetContext()->GetReadMetadata()->GetIndexInfo().AddSpecialColumns(batch, CommittedBlob.GetSnapshot());
    MutableStageData().AddBatch(batch);
}

}
