#include "fetched_data.h"
#include "interval.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessor_callback.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NPlain {

void IDataSource::InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching) {
    AFL_VERIFY(fetching);
    //    AFL_VERIFY(!FetchingPlan);
    FetchingPlan = fetching;
}

void IDataSource::RegisterInterval(TFetchingInterval& interval, const std::shared_ptr<IDataSource>& sourcePtr) {
    if (!IsReadyFlag) {
        AFL_VERIFY(Intervals.emplace(interval.GetIntervalIdx(), &interval).second);
    }
    if (AtomicCas(&SourceStartedFlag, 1, 0)) {
        SetMemoryGroupId(interval.GetIntervalId());
        AFL_VERIFY(FetchingPlan);
        InitStageData(std::make_unique<TFetchedData>(GetExclusiveIntervalOnly(), GetRecordsCount()));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", FetchingPlan->DebugString())("source_idx", GetSourceIdx());
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", GetSourceIdx())("method", "InitFetchingPlan"));
        if (GetContext()->IsAborted()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "InitFetchingPlanAborted");
            return;
        }
        TFetchingScriptCursor cursor(FetchingPlan, 0);
        const auto& commonContext = *GetContext()->GetCommonContext();
        auto task = std::make_shared<TStepAction>(sourcePtr, std::move(cursor), commonContext.GetScanActorId(), true);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, commonContext.GetConveyorProcessId());
    }
}

void IDataSource::DoOnSourceFetchingFinishedSafe(IDataReader& /*owner*/, const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    AFL_VERIFY(!IsReadyFlag);
    IsReadyFlag = true;
    for (auto&& i : Intervals) {
        i.second->OnSourceFetchStageReady(GetSourceIdx());
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "source_ready")("intervals_count", Intervals.size())("source_idx", GetSourceIdx());
    Intervals.clear();
}

void IDataSource::DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    if (ResourceGuards.size()) {
        if (ExclusiveIntervalOnly) {
            ResourceGuards.back()->Update(0);
        } else {
            ResourceGuards.back()->Update(GetColumnRawBytes(GetContext()->GetMergeColumns()->GetColumnIds()));
        }
    }
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT_EMPTY", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    if (ExclusiveIntervalOnly) {
        StageResult = TFetchedResult::BuildEmpty();
    } else {
        StageResult = std::make_unique<TFetchedResult>(
            ExtractStageData(), GetContext()->GetMergeColumns()->GetColumnIds(), *GetContext()->GetCommonContext()->GetResolver());
    }
    ClearStageData();
}

void IDataSource::DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    StageResult = std::make_unique<TFetchedResult>(ExtractStageData(), *GetContext()->GetCommonContext()->GetResolver());
    ClearStageData();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
    THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& defaultBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter) {
    const NArrow::TColumnFilter& cFilter = filter ? *filter : NArrow::TColumnFilter::BuildAllowFilter();
    ui32 fetchedChunks = 0;
    ui32 nullChunks = 0;
    for (auto&& i : columnIds) {
        auto columnChunks = GetPortionAccessor().GetColumnChunksPointers(i);
        if (columnChunks.empty()) {
            continue;
        }
        auto itFilter = cFilter.GetBegin(false, Portion->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                auto reading = blobsAction.GetReading(Portion->GetColumnStorageId(c->GetColumnId(), Schema->GetIndexInfo()));
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(GetPortionAccessor().RestoreBlobRange(c->BlobRange));
                ++fetchedChunks;
            } else {
                defaultBlocks.emplace(c->GetAddress(), TPortionDataAccessor::TAssembleBlobInfo(c->GetMeta().GetRecordsCount(),
                                                           Schema->GetExternalDefaultValueVerified(c->GetColumnId())));
                ++nullChunks;
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->GetRecordsCount());
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "chunks_stats")("fetch", fetchedChunks)("null", nullChunks)(
        "reading_actions", blobsAction.GetStorageIds())("columns", columnIds.size());
}

bool TPortionDataSource::DoStartFetchingColumns(
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName());
    AFL_VERIFY(columns.GetColumnsCount());
    AFL_VERIFY(!GetStageData().GetAppliedFilter() || !GetStageData().GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns.GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, GetStageData().GetAppliedFilter());
        MutableStageData().AddDefaults(std::move(nullBlocks));
    }

    auto readActions = action.GetReadingActions();
    if (!readActions.size()) {
        return false;
    }

    auto constructor =
        std::make_shared<NCommon::TBlobsFetcherTask>(readActions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TPortionDataSource::DoAbort() {
}

void TPortionDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) {
    auto blobSchema = GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*Portion);

    std::optional<TSnapshot> ss;
    if (Portion->GetPortionType() == EPortionType::Written) {
        const auto* portion = static_cast<const TWrittenPortionInfo*>(Portion.get());
        if (portion->HasCommitSnapshot()) {
            ss = portion->GetCommitSnapshotVerified();
        } else if (GetContext()->GetReadMetadata()->IsMyUncommitted(portion->GetInsertWriteId())) {
            ss = GetContext()->GetReadMetadata()->GetRequestSnapshot();
        }
    }

    auto batch = GetPortionAccessor()
                     .PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs(), ss)
                     .AssembleToGeneralContainer(sequential ? columns->GetColumnIds() : std::set<ui32>())
                     .DetachResult();
    MutableStageData().AddBatch(batch, *GetContext()->GetCommonContext()->GetResolver(), true);
}

bool TPortionDataSource::DoStartFetchingAccessor(const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
    AFL_VERIFY(!HasPortionAccessor());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<TDataAccessorsRequest> request =
        std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    request->AddPortion(Portion);
    request->RegisterSubscriber(std::make_shared<NCommon::TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

bool TPortionDataSource::DoAddTxConflict() {
    if (Portion->IsCommitted()) {
        GetContext()->GetReadMetadata()->SetBrokenWithCommitted();
        return true;
    } else {
        const auto* wPortion = static_cast<const TWrittenPortionInfo*>(Portion.get());
        if (!GetContext()->GetReadMetadata()->IsMyUncommitted(wPortion->GetInsertWriteId())) {
            GetContext()->GetReadMetadata()->SetConflictedWriteId(wPortion->GetInsertWriteId());
            return true;
        }
    }
    return false;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
