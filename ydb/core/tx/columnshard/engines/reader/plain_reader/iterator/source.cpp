#include "fetched_data.h"
#include "interval.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
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
        StageData = std::make_unique<TFetchedData>(GetExclusiveIntervalOnly(), GetRecordsCount());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", FetchingPlan->DebugString())("source_idx", GetSourceIdx());
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", GetSourceIdx())("method", "InitFetchingPlan"));
        if (GetContext()->IsAborted()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "InitFetchingPlanAborted");
            return;
        }
        TFetchingScriptCursor cursor(FetchingPlan, 0);
        auto task = std::make_shared<TStepAction>(sourcePtr, std::move(cursor), GetContext()->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
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
            std::move(StageData), GetContext()->GetMergeColumns()->GetColumnIds(), *GetContext()->GetCommonContext()->GetResolver());
    }
    StageData.reset();
}

void IDataSource::DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    StageResult = std::make_unique<TFetchedResult>(std::move(StageData), *GetContext()->GetCommonContext()->GetResolver());
    StageData.reset();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
    THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& defaultBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter) {
    const NArrow::TColumnFilter& cFilter = filter ? *filter : NArrow::TColumnFilter::BuildAllowFilter();
    ui32 fetchedChunks = 0;
    ui32 nullChunks = 0;
    for (auto&& i : columnIds) {
        auto columnChunks = GetStageData().GetPortionAccessor().GetColumnChunksPointers(i);
        if (columnChunks.empty()) {
            continue;
        }
        auto itFilter = cFilter.GetIterator(false, Portion->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                auto reading = blobsAction.GetReading(Portion->GetColumnStorageId(c->GetColumnId(), Schema->GetIndexInfo()));
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Portion->RestoreBlobRange(c->BlobRange));
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
    AFL_VERIFY(!StageData->GetAppliedFilter() || !StageData->GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns.GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, StageData->GetAppliedFilter());
        StageData->AddDefaults(std::move(nullBlocks));
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
    if (Portion->HasInsertWriteId()) {
        if (Portion->HasCommitSnapshot()) {
            ss = Portion->GetCommitSnapshotVerified();
        } else if (GetContext()->GetReadMetadata()->IsMyUncommitted(Portion->GetInsertWriteIdVerified())) {
            ss = GetContext()->GetReadMetadata()->GetRequestSnapshot();
        }
    }

    auto batch = GetStageData()
                     .GetPortionAccessor()
                     .PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs(), ss)
                     .AssembleToGeneralContainer(sequential ? columns->GetColumnIds() : std::set<ui32>())
                     .DetachResult();
    MutableStageData().AddBatch(batch, *GetContext()->GetCommonContext()->GetResolver(), true);
}

namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    TFetchingScriptCursor Step;
    std::shared_ptr<IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Source->GetContext()->GetCommonContext()->GetAbortionFlag();
    }
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(!result.HasErrors());
        AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
        Source->MutableStageData().SetPortionAccessor(std::move(result.ExtractPortionsVector().front()));
        AFL_VERIFY(Step.Next());
        auto task = std::make_shared<TStepAction>(Source, std::move(Step), Source->GetContext()->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }

public:
    TPortionAccessorFetchingSubscriber(const TFetchingScriptCursor& step, const std::shared_ptr<IDataSource>& source)
        : Step(step)
        , Source(source)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    }
};
}   // namespace

bool TPortionDataSource::DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
    AFL_VERIFY(!StageData->HasPortionAccessor());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("PLAIN::" + step.GetName());
    request->AddPortion(Portion);
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

bool TCommittedDataSource::DoStartFetchingColumns(
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& /*columns*/) {
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
    auto constructor = std::make_shared<NCommon::TBlobsFetcherTask>(actions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

void TCommittedDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool /*sequential*/) {
    TMemoryProfileGuard mGuard("SCAN_PROFILE::ASSEMBLER::COMMITTED", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    const ISnapshotSchema::TPtr batchSchema =
        GetContext()->GetReadMetadata()->GetIndexVersions().GetSchemaVerified(GetCommitted().GetSchemaVersion());
    const ISnapshotSchema::TPtr resultSchema = GetContext()->GetReadMetadata()->GetResultSchema();
    if (!GetStageData().GetTable()->HasAccessors()) {
        AFL_VERIFY(GetStageData().GetBlobs().size() == 1);
        auto bData = MutableStageData().ExtractBlob(GetStageData().GetBlobs().begin()->first);
        auto schema = GetContext()->GetReadMetadata()->GetBlobSchema(CommittedBlob.GetSchemaVersion());
        auto rBatch = NArrow::DeserializeBatch(
            bData, std::make_shared<arrow::Schema>(CommittedBlob.GetSchemaSubset().Apply(schema.begin(), schema.end())));
        AFL_VERIFY(rBatch)("schema", schema.ToString());
        auto batch = std::make_shared<NArrow::TGeneralContainer>(rBatch);
        std::set<ui32> columnIdsToDelete = batchSchema->GetColumnIdsToDelete(resultSchema);
        if (!columnIdsToDelete.empty()) {
            batch->DeleteFieldsByIndex(batchSchema->ConvertColumnIdsToIndexes(columnIdsToDelete));
        }
        TSnapshot ss = TSnapshot::Zero();
        if (CommittedBlob.IsCommitted()) {
            ss = CommittedBlob.GetCommittedSnapshotVerified();
        } else {
            ss = GetContext()->GetReadMetadata()->IsMyUncommitted(CommittedBlob.GetInsertWriteId())
                     ? GetContext()->GetReadMetadata()->GetRequestSnapshot()
                     : TSnapshot::Zero();
        }
        GetContext()->GetReadMetadata()->GetIndexInfo().AddSnapshotColumns(*batch, ss, (ui64)CommittedBlob.GetInsertWriteId());
        GetContext()->GetReadMetadata()->GetIndexInfo().AddDeleteFlagsColumn(*batch, CommittedBlob.GetIsDelete());
        MutableStageData().AddBatch(batch, *GetContext()->GetCommonContext()->GetResolver(), true);
        if (CommittedBlob.GetIsDelete()) {
            MutableStageData().AddFilter(NArrow::TColumnFilter::BuildDenyFilter());
        }
    }
    MutableStageData().SyncTableColumns(columns->GetSchema()->fields(), *resultSchema, GetRecordsCount());
}

}   // namespace NKikimr::NOlap::NReader::NPlain
