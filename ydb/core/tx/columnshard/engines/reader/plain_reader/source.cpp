#include "source.h"
#include "interval.h"
#include "fetched_data.h"
#include "constructor.h"
#include "plain_read_data.h"
#include <ydb/core/formats/arrow/serializer/full.h>

namespace NKikimr::NOlap::NPlainReader {

void IDataSource::InitFetchStageData(const std::shared_ptr<arrow::RecordBatch>& batchExt) {
    auto batch = batchExt;
    if (!batch && FetchingPlan->GetFetchingStage()->GetSize()) {
        const ui32 numRows = GetFilterStageData().GetBatch() ? GetFilterStageData().GetBatch()->num_rows() : 0;
        batch = NArrow::MakeEmptyBatch(FetchingPlan->GetFetchingStage()->GetSchema(), numRows);
    }
    if (batch) {
        Y_VERIFY((ui32)batch->num_columns() == FetchingPlan->GetFetchingStage()->GetSize());
    }
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchStageData"));
    Y_VERIFY(!FetchStageData);
    FetchStageData = std::make_shared<TFetchStageData>(batch);
    auto intervals = Intervals;
    for (auto&& i : intervals) {
        i->OnSourceFetchStageReady(GetSourceIdx());
    }
}

void IDataSource::InitFilterStageData(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter, const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch) {
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFilterStageData"));
    Y_VERIFY(!FilterStageData);
    FilterStageData = std::make_shared<TFilterStageData>(appliedFilter, earlyFilter, batch);
    if (batch) {
        Y_VERIFY((ui32)batch->num_columns() == FetchingPlan->GetFilterStage()->GetSize());
    }
    auto intervals = Intervals;
    for (auto&& i : intervals) {
        i->OnSourceFilterStageReady(GetSourceIdx());
    }
    DoStartFetchStage();
}

void IDataSource::InitFetchingPlan(const TFetchingPlan& fetchingPlan) {
    if (!FilterStageFlag) {
        FilterStageFlag = true;
        Y_VERIFY(!FetchingPlan);
        FetchingPlan = fetchingPlan;
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
        DoStartFilterStage();
    }
}

bool IDataSource::OnIntervalFinished(const ui32 intervalIdx) {
    Y_VERIFY(Intervals.size());
    Y_VERIFY(Intervals.front()->GetIntervalIdx() == intervalIdx);
    Intervals.pop_front();
    return Intervals.empty();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds,
    const std::shared_ptr<IBlobsReadingAction>& readingAction, THashMap<TBlobRange, ui32>& nullBlocks,
    const std::shared_ptr<NArrow::TColumnFilter>& filter) {
    const NArrow::TColumnFilter& cFilter = filter ? *filter : NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& i : columnIds) {
        auto columnChunks = Portion->GetColumnChunksPointers(i);
        if (columnChunks.empty()) {
            continue;
        }
        auto itFilter = cFilter.GetIterator(false, Portion->NumRows(i));
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            Y_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetNumRowsVerified())) {
                readingAction->AddRange(c->BlobRange);
            } else {
                nullBlocks.emplace(c->BlobRange, c->GetMeta().GetNumRowsVerified());
            }
            itFinished = !itFilter.Next(c->GetMeta().GetNumRowsVerified());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->NumRows(i));
    }
}

void TPortionDataSource::DoStartFilterStage() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetchEF");
    Y_VERIFY(FetchingPlan->GetFilterStage()->GetSize());
    auto& columnIds = FetchingPlan->GetFilterStage()->GetColumnIds();

    auto readAction = Portion->GetBlobsStorage()->StartReadingAction();
    THashMap<TBlobRange, ui32> nullBlocks;
    NeedFetchColumns(columnIds, readAction, nullBlocks, nullptr);

    std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
    auto constructor = std::make_shared<TEFTaskConstructor>(ReadData, actions, std::move(nullBlocks), columnIds, *this, FetchingPlan->CanUseEarlyFilterImmediately());
    ReadData.AddForFetch(GetSourceIdx(), constructor, false);
}

void TPortionDataSource::DoStartFetchStage() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoStartFetchStage");
    Y_VERIFY(!FetchStageData);
    Y_VERIFY(FilterStageData);
    if (FetchingPlan->GetFetchingStage()->GetSize() && !FilterStageData->IsEmptyFilter()) {
        auto& columnIds = FetchingPlan->GetFetchingStage()->GetColumnIds();

        auto readAction = Portion->GetBlobsStorage()->StartReadingAction();
        THashMap<TBlobRange, ui32> nullBlocks;
        NeedFetchColumns(columnIds, readAction, nullBlocks, GetFilterStageData().GetActualFilter());
        if (readAction->GetExpectedBlobsCount()) {
            std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
            auto constructor = std::make_shared<TFFColumnsTaskConstructor>(ReadData, actions, std::move(nullBlocks), columnIds, *this);
            ReadData.AddForFetch(GetSourceIdx(), constructor, true);
            return;
        }
    }
    InitFetchStageData(nullptr);
}

void TPortionDataSource::DoAbort() {
}

void TCommittedDataSource::DoFetch() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoFetch");
    if (!ReadStarted) {
        Y_VERIFY(!ResultReady);
        ReadStarted = true;

        std::shared_ptr<IBlobsStorageOperator> storageOperator = ReadData.GetContext().GetStoragesManager()->GetInsertOperator();
        auto readAction = storageOperator->StartReadingAction();
        readAction->AddRange(CommittedBlob.GetBlobRange());

        THashMap<TBlobRange, ui32> nullBlocks;
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readAction};
        auto constructor = std::make_shared<TCommittedColumnsTaskConstructor>(ReadData, actions, std::move(nullBlocks), *this);
        ReadData.AddForFetch(GetSourceIdx(), constructor, true);
    }
}

}
