#include "interval.h"
#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

bool TFetchingInterval::IsExclusiveInterval() const {
    return TBase::IsExclusiveInterval(Sources.size());
}

class TEmptyMergeTask: public NColumnShard::IDataTasksProcessor::ITask, public TMergingContext {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
protected:
    virtual bool DoApply(NOlap::IDataReader& indexedDataRead) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", IntervalIdx);
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        reader.MutableScanner().OnIntervalResult(nullptr, IntervalIdx, reader);
        return true;
    }
    virtual bool DoExecute() override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", IntervalIdx);
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_RESULT::EMPTY";
    }

    TEmptyMergeTask(const TMergingContext& context, const TActorId& scanActorId)
        : TBase(scanActorId)
        , TMergingContext(context)
    {

    }

};

class TMergeTask: public NColumnShard::IDataTasksProcessor::ITask, public TMergingContext {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    std::shared_ptr<NIndexedReader::TMergePartialStream> Merger;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;
    const NColumnShard::TCounterGuard Guard;
    const ui32 OriginalSourcesCount;
    std::shared_ptr<TSpecialReadContext> Context;
protected:
    virtual bool DoApply(NOlap::IDataReader& indexedDataRead) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", IntervalIdx);
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        if (Merger->GetSourcesCount() == 1 && ResultBatch) {
            auto batch = NArrow::ExtractColumnsValidate(ResultBatch, reader.GetSpecialReadContext()->GetResultFieldNames());
            AFL_VERIFY(batch);
            reader.MutableScanner().OnIntervalResult(batch, IntervalIdx, reader);
        } else {
            reader.MutableScanner().OnIntervalResult(ResultBatch, IntervalIdx, reader);
        }
        return true;
    }
    virtual bool DoExecute() override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", IntervalIdx);
        Merger->SkipToLowerBound(Start, IncludeStart);
        if (Merger->GetSourcesCount() == 1) {
            ResultBatch = Merger->SingleSourceDrain(Finish, IncludeFinish);
            if (ResultBatch) {
                if (IsExclusiveInterval(1)) {
                    Context->GetCommonContext()->GetCounters().OnNoScanInterval(ResultBatch->num_rows());
                } else {
                    Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
                }
            }
            if (IncludeFinish && OriginalSourcesCount == 1) {
                Y_ABORT_UNLESS(Merger->IsEmpty());
            }
        } else {
            auto rbBuilder = std::make_shared<NIndexedReader::TRecordBatchBuilder>(Context->GetResultFields());
            Merger->DrainCurrentTo(*rbBuilder, Finish, IncludeFinish);
            Context->GetCommonContext()->GetCounters().OnLinearScanInterval(rbBuilder->GetRecordsCount());
            ResultBatch = rbBuilder->Finalize();
        }
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_RESULT";
    }

    TMergeTask(const std::shared_ptr<NIndexedReader::TMergePartialStream>& merger,
        const TMergingContext& context, const std::shared_ptr<TSpecialReadContext>& readContext, const ui32 sourcesCount)
        : TBase(readContext->GetCommonContext()->GetScanActorId())
        , TMergingContext(context)
        , Merger(merger)
        , Guard(readContext->GetCommonContext()->GetCounters().GetMergeTasksGuard())
        , OriginalSourcesCount(sourcesCount)
        , Context(readContext)
    {
    }
};

void TFetchingInterval::ConstructResult() {
    if (!IsSourcesReady() || !ResourcesGuard) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_construct_result")("interval_idx", IntervalIdx);
        return;
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "start_construct_result")("interval_idx", IntervalIdx);
    }
    AFL_VERIFY(!ResultConstructionInProgress)("interval_idx", IntervalIdx);
    ResultConstructionInProgress = true;
    auto merger = Context->BuildMerger();
    for (auto&& [_, i] : Sources) {
        if (auto rb = i->GetBatch()) {
            merger->AddSource(rb, i->GetFilterStageData().GetNotAppliedEarlyFilter());
        }
    }
    AFL_VERIFY(merger->GetSourcesCount() <= Sources.size());
    std::shared_ptr<NConveyor::ITask> task;
    if (merger->GetSourcesCount() == 0) {
        task = std::make_shared<TEmptyMergeTask>(*this, Context->GetCommonContext()->GetScanActorId());
    } else {
        task = std::make_shared<TMergeTask>(merger, *this, Context, Sources.size());
    }
    task->SetPriority(NConveyor::ITask::EPriority::High);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

void TFetchingInterval::OnInitResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "allocated")("interval_idx", IntervalIdx);
    AFL_VERIFY(guard);
    AFL_VERIFY(!ResourcesGuard);
    ResourcesGuard = guard;
    ConstructResult();
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetched")("interval_idx", IntervalIdx);
    ConstructResult();
}

void TFetchingInterval::OnSourceFilterStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "filtered")("interval_idx", IntervalIdx);
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
    const bool includeFinish, const bool includeStart)
    : TBase(start, finish, intervalIdx, includeFinish, includeStart)
    , TTaskBase(0, context->GetMemoryForSources(sources, TBase::IsExclusiveInterval(sources.size())), "", context->GetCommonContext()->GetResourcesTaskContext())
    , Context(context)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , Sources(sources)
{
    Y_ABORT_UNLESS(Sources.size());
    for (auto&& [_, i] : Sources) {
        i->RegisterInterval(this);
    }
}

void TFetchingInterval::DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("interval_idx", IntervalIdx)("event", "resources_allocated")
        ("resources", guard->DebugString())("start", IncludeStart)("finish", IncludeFinish)("sources", Sources.size());
    OnInitResourcesGuard(guard);
    for (auto&& [_, i] : Sources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(IsExclusiveInterval()));
    }
}

}
