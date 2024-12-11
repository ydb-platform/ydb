#include "constructor.h"
#include "fetched_data.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

void IDataSource::InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching) {
    AFL_VERIFY(fetching);
    //    AFL_VERIFY(!FetchingPlan);
    FetchingPlan = fetching;
}

void IDataSource::StartProcessing(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(!ProcessingStarted);
    AFL_VERIFY(FetchingPlan);
    AFL_VERIFY(!Context->IsAborted());
    ProcessingStarted = true;
    SourceGroupGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildGroupGuard(
        GetContext()->GetProcessMemoryControlId(), GetContext()->GetCommonContext()->GetScanId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", FetchingPlan->DebugString())("source_idx", SourceIdx);
//    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
    TFetchingScriptCursor cursor(FetchingPlan, 0);
    auto task = std::make_shared<TStepAction>(sourcePtr, std::move(cursor), Context->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

void IDataSource::ContinueCursor(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(!!ScriptCursor);
    if (ScriptCursor->Next()) {
        auto task = std::make_shared<TStepAction>(sourcePtr, std::move(*ScriptCursor), Context->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
        ScriptCursor.reset();
    }
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
    const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
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

    auto constructor = std::make_shared<TBlobsFetcherTask>(readActions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

bool TPortionDataSource::DoStartFetchingIndexes(
    const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TIndexesSet>& indexes) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName());
    AFL_VERIFY(indexes->GetIndexesCount());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        std::set<ui32> indexIds;
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            if (!indexes->GetIndexIdsSet().contains(i.GetIndexId())) {
                continue;
            }
            indexIds.emplace(i.GetIndexId());
            if (auto bRange = i.GetBlobRangeOptional()) {
                auto readAction = action.GetReading(Portion->GetIndexStorageId(i.GetIndexId(), Schema->GetIndexInfo()));
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
    std::vector<TPortionDataAccessor::TPage> pages = GetStageData().GetPortionAccessor().BuildPages();
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

    MutableStageData().AddBatch(batch);
}

namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    TFetchingScriptCursor Step;
    std::shared_ptr<IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
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

    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>();
    request->AddPortion(Portion);
    request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

TPortionDataSource::TPortionDataSource(
    const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context)
    : TBase(portion->GetPortionId(), sourceIdx, context, portion->IndexKeyStart(), portion->IndexKeyEnd(),
          portion->RecordSnapshotMin(TSnapshot::Zero()), portion->RecordSnapshotMax(TSnapshot::Zero()), portion->GetRecordsCount(),
          portion->GetShardingVersionOptional(), portion->GetMeta().GetDeletionsCount())
    , Portion(portion)
    , Schema(GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*portion)) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple
