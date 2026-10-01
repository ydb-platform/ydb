#include "fetched_data.h"
#include "interval.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessor_callback.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NPlain {

void IDataSource::InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching) {
    AFL_VERIFY(fetching);
    FetchingPlan = fetching;
}

void IDataSource::StartProcessing(std::unique_ptr<NCommon::TDataSourceLease> sourceLease, const ui64 memoryGroupId) {
    auto& self = *sourceLease->GetSource().MutableAs<IDataSource>();
    self.OnStartProcessing();
    self.SetMemoryGroupId(memoryGroupId);
    AFL_VERIFY(self.FetchingPlan);
    self.InitStageData(std::make_unique<TFetchedData>(self.GetExclusiveIntervalOnly(), self.GetRecordsCount()));
    YDB_LOG_DEBUG("",
        {"initFetchingPlan", self.FetchingPlan->DebugString()},
        {"sourceIdx", self.GetSourceIdx()});
    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", self.GetSourceIdx())("method", "StartProcessing"));
    if (self.GetContext()->IsAborted()) {
        YDB_LOG_DEBUG("",
            {"event", "StartProcessingAborted"});
        return;
    }
    TFetchingScriptCursor cursor(self.FetchingPlan, 0);
    const auto& commonContext = *self.GetContext()->GetCommonContext();
    auto task = std::make_shared<TStepAction>(std::move(sourceLease), std::move(cursor), commonContext.GetScanActorId(), true);
    commonContext.SendTaskToExecute(task);
}

void IDataSource::DoOnSourceFetchingFinishedSafe(IDataReader& owner, std::unique_ptr<NCommon::TDataSourceLease> self) {
    AFL_VERIFY(!IsReadyFlag);
    IsReadyFlag = true;
    static_cast<TPlainReadData&>(owner).MutableScanner().OnSourceReady(std::move(self));
}

void IDataSource::DoOnEmptyStageData() {
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

void IDataSource::DoBuildStageResult() {
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
    YDB_LOG_DEBUG("",
        {"event", "chunks_stats"},
        {"fetch", fetchedChunks},
        {"null", nullChunks},
        {"readingActions", blobsAction.GetStorageIds()},
        {"columns", columnIds.size()});
}

NCommon::TExecutionResult TPortionDataSource::DoStartFetchingColumns(const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
    YDB_LOG_DEBUG("",
        {"event", step.GetName()});
    AFL_VERIFY(columns.GetColumnsCount());
    AFL_VERIFY(!GetStageData().GetAppliedFilter() || !GetStageData().GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns.GetColumnIds();
    YDB_LOG_DEBUG("",
        {"event", step.GetName()},
        {"fetchingInfo", step.DebugString()});

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, GetStageData().GetAppliedFilter());
        MutableStageData().AddDefaults(std::move(nullBlocks));
    }

    auto readActions = action.GetReadingActions();
    if (!readActions.size()) {
        return NCommon::TExecutionResult::Done();
    }

    return NCommon::TExecutionResult::Pending(
        std::make_shared<NCommon::TBlobsFetcherTask::TStartJob>(readActions, step, "CS::READ::" + step.GetName()));
}

void TPortionDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) {
    auto blobSchema = GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*Portion);

    std::optional<TSnapshot> ss;
    if (Portion->GetPortionType() == EPortionType::Written) {
        const auto* portion = static_cast<const TWrittenPortionInfo*>(Portion.get());
        if (portion->HasCommitSnapshot()) {
            ss = portion->GetCommitSnapshotVerified();
        } else if (!IsConflicting()) {
            // if a portion is not committed, and not conflicting, it is a portion written by the current tx
            ss = GetContext()->GetReadMetadata()->GetRequestSnapshot();
        }
    }

    auto batch = GetPortionAccessor()
                     .PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs(), ss)
                     .AssembleToGeneralContainer(sequential ? columns->GetColumnIds() : std::set<ui32>())
                     .DetachResult();
    MutableStageData().AddBatch(batch, *GetContext()->GetCommonContext()->GetResolver(), true);
}

NCommon::TExecutionResult TPortionDataSource::DoStartFetchingAccessor(const TFetchingScriptCursor& step) {
    AFL_VERIFY(!HasPortionAccessor());
    YDB_LOG_DEBUG("",
        {"event", step.GetName()},
        {"fetchingInfo", step.DebugString()});

    std::shared_ptr<TDataAccessorsRequest> request =
        std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    request->AddPortion(Portion);
    return NCommon::TExecutionResult::Pending(std::make_shared<NCommon::TPortionAccessorFetchingSubscriber::TStartJob>(
        GetContext()->GetCommonContext()->GetDataAccessorsManager(), std::move(request), step));
}

bool TPortionDataSource::DoAddTxConflict() {
    if (!IsConflicting()) {
        return false;
    }
    auto& info = GetPortionInfo();
    if (info.IsCommitted()) {
        // conflicting portion got aborted, so it doesn't conflict with us anymore
        // but we return true here anyway because it is what the caller expects for a
        // portion we don't want to read
        if (info.IsAborted()) {
            return true;
        }
        // conflicting portion is already committed, we don't have a chance to commit anymore
        GetContext()->GetReadMetadata()->BreakLock();
        return true;
    } else {
        // conflicting portion is not committed yet, remember it
        const auto* wPortion = static_cast<const TWrittenPortionInfo*>(Portion.get());
        GetContext()->GetReadMetadata()->SetWriteConflicting(wPortion->GetInsertWriteId());
        return true;
    }
}

}   // namespace NKikimr::NOlap::NReader::NPlain
