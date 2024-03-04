#include "source.h"
#include "interval.h"
#include "fetched_data.h"
#include "constructor.h"
#include "plain_read_data.h"
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>

namespace NKikimr::NOlap::NPlainReader {

void IDataSource::InitFetchStageData(const std::shared_ptr<arrow::RecordBatch>& batchExt) {
    auto batch = batchExt;
    if (!batch && FetchingPlan->GetFetchingStage()->GetSize()) {
        const ui32 numRows = GetFilterStageData().GetBatch() ? GetFilterStageData().GetBatch()->num_rows() : 0;
        batch = NArrow::MakeEmptyBatch(FetchingPlan->GetFetchingStage()->GetSchema(), numRows);
    }
    if (batch) {
        Y_ABORT_UNLESS((ui32)batch->num_columns() == FetchingPlan->GetFetchingStage()->GetSize());
    }
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchStageData"));
    Y_ABORT_UNLESS(!FetchStageData);
    FetchStageData = std::make_shared<TFetchStageData>(batch);
    for (auto&& i : Intervals) {
        i.second->OnSourceFetchStageReady(GetSourceIdx());
    }
    Intervals.clear();
}

void IDataSource::InitFilterStageData(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter,
    const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<IDataSource>& sourcePtr) {
    if (IsAborted()) {
        return;
    }
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFilterStageData"));
    Y_ABORT_UNLESS(!FilterStageData);
    FilterStageData = std::make_shared<TFilterStageData>(appliedFilter, earlyFilter, batch);
    if (batch) {
        Y_ABORT_UNLESS((ui32)batch->num_columns() == FetchingPlan->GetFilterStage()->GetSize());
    }
    DoStartFetchStage(sourcePtr);
}

void IDataSource::InitFetchingPlan(const TFetchingPlan& fetchingPlan, const std::shared_ptr<IDataSource>& sourcePtr) {
    if (AtomicCas(&FilterStageFlag, 1, 0)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", fetchingPlan.DebugString());
        Y_ABORT_UNLESS(!FetchingPlan);
        FetchingPlan = fetchingPlan;
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
        if (IsAborted()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "InitFetchingPlanAborted");
            return;
        }
        DoStartFilterStage(sourcePtr);
    }
}

void IDataSource::RegisterInterval(TFetchingInterval* interval) {
    if (!FetchStageData) {
        AFL_VERIFY(interval);
        AFL_VERIFY(Intervals.emplace(interval->GetIntervalIdx(), interval).second);
    }
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

void TPortionDataSource::DoStartFilterStage(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetchEF");
    Y_ABORT_UNLESS(FetchingPlan->GetFilterStage()->GetSize());
    auto& columnIds = FetchingPlan->GetFilterStage()->GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "StartFilterStage")("fetching_info", FetchingPlan->DebugString());

    auto readAction = Portion->GetBlobsStorage()->StartReadingAction("CS::READ::FILTER");
    readAction->SetIsBackgroundProcess(false);
    THashMap<TBlobRange, ui32> nullBlocks;
    NeedFetchColumns(columnIds, readAction, nullBlocks, nullptr);

    std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
    auto constructor = std::make_shared<TEFTaskConstructor>(GetContext(), actions, std::move(nullBlocks), columnIds, *this, sourcePtr, FetchingPlan->CanUseEarlyFilterImmediately(), "ReaderFilter");
//    NActors::TActivationContext::AsActorContext().Send(GetContext()->GetCommonContext()->GetReadCoordinatorActorId(), new NOlap::NBlobOperations::NRead::TEvStartReadTask(constructor));
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
}

void TPortionDataSource::DoStartFetchStage(const std::shared_ptr<IDataSource>& sourcePtr) {
    Y_ABORT_UNLESS(!FetchStageData);
    Y_ABORT_UNLESS(FilterStageData);
    if (FetchingPlan->GetFetchingStage()->GetSize() && !FilterStageData->IsEmptyFilter()) {
        auto& columnIds = FetchingPlan->GetFetchingStage()->GetColumnIds();

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "RealStartFetchStage")("fetching_info", FetchingPlan->DebugString());
        auto readAction = Portion->GetBlobsStorage()->StartReadingAction("CS::READ::FETCHING");
        readAction->SetIsBackgroundProcess(false);
        THashMap<TBlobRange, ui32> nullBlocks;
        NeedFetchColumns(columnIds, readAction, nullBlocks, GetFilterStageData().GetAppliedFilter());
        if (readAction->GetExpectedBlobsCount()) {
            std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
            auto constructor = std::make_shared<TFFColumnsTaskConstructor>(GetContext(), actions, std::move(nullBlocks), columnIds, *this, sourcePtr, "ReaderFetcher");
            NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
            return;
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DontStartFetchStage")("fetching_info", FetchingPlan->DebugString());
    }
    InitFetchStageData(nullptr);
}

void TPortionDataSource::DoAbort() {
}

void TCommittedDataSource::DoFetch(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetch");
    if (!ReadStarted) {
        Y_ABORT_UNLESS(!ResultReady);
        ReadStarted = true;

        std::shared_ptr<IBlobsStorageOperator> storageOperator = GetContext()->GetCommonContext()->GetStoragesManager()->GetInsertOperator();
        auto readAction = storageOperator->StartReadingAction("CS::READ::COMMITTED");
        readAction->SetIsBackgroundProcess(false);
        readAction->AddRange(CommittedBlob.GetBlobRange());

        THashMap<TBlobRange, ui32> nullBlocks;
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
        auto constructor = std::make_shared<TCommittedColumnsTaskConstructor>(GetContext(), actions, std::move(nullBlocks), *this, sourcePtr, "ReaderCommitted");
        NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    }
}

}
