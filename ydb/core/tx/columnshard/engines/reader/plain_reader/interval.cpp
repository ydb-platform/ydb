#include "interval.h"
#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

class TMergeTask: public NColumnShard::IDataTasksProcessor::ITask {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;
    const NColumnShard::TCounterGuard Guard;
    std::shared_ptr<TSpecialReadContext> Context;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;
    std::shared_ptr<TMergingContext> MergingContext;
    const ui32 IntervalIdx;
protected:
    virtual bool DoApply(NOlap::IDataReader& indexedDataRead) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", MergingContext->GetIntervalIdx());
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        reader.MutableScanner().OnIntervalResult(ResultBatch, IntervalIdx, reader);
        return true;
    }
    virtual bool DoExecute() override {
        std::shared_ptr<NIndexedReader::TMergePartialStream> merger = Context->BuildMerger();
        for (auto&& [_, i] : Sources) {
            if (auto rb = i->GetBatch()) {
                merger->AddSource(rb, i->GetFilterStageData().GetNotAppliedEarlyFilter());
            }
        }
        AFL_VERIFY(merger->GetSourcesCount() <= Sources.size());
        const ui32 originalSourcesCount = Sources.size();
        Sources.clear();

        if (merger->GetSourcesCount() == 0) {
            ResultBatch = nullptr;
            return true;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", MergingContext->GetIntervalIdx());
        merger->SkipToLowerBound(MergingContext->GetStart(), MergingContext->GetIncludeStart());
        if (merger->GetSourcesCount() == 1) {
            ResultBatch = merger->SingleSourceDrain(MergingContext->GetFinish(), MergingContext->GetIncludeFinish());
            if (ResultBatch) {
                if (MergingContext->IsExclusiveInterval(1)) {
                    Context->GetCommonContext()->GetCounters().OnNoScanInterval(ResultBatch->num_rows());
                } else {
                    Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
                }
                Y_ABORT_UNLESS(ResultBatch->schema()->Equals(Context->GetResultSchema()));
            }
            if (MergingContext->GetIncludeFinish() && originalSourcesCount == 1) {
                AFL_VERIFY(merger->IsEmpty())("merging_context_finish", MergingContext->GetFinish().DebugJson().GetStringRobust())("merger", merger->DebugString());
            }
        } else {
            auto rbBuilder = std::make_shared<NIndexedReader::TRecordBatchBuilder>(Context->GetResultFields());
            merger->DrainCurrentTo(*rbBuilder, MergingContext->GetFinish(), MergingContext->GetIncludeFinish());
            Context->GetCommonContext()->GetCounters().OnLinearScanInterval(rbBuilder->GetRecordsCount());
            ResultBatch = rbBuilder->Finalize();
        }
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_RESULT";
    }

    TMergeTask(std::shared_ptr<TMergingContext>&& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext, std::map<ui32, std::shared_ptr<IDataSource>>&& sources)
        : TBase(readContext->GetCommonContext()->GetScanActorId())
        , Guard(readContext->GetCommonContext()->GetCounters().GetMergeTasksGuard())
        , Context(readContext)
        , Sources(std::move(sources))
        , MergingContext(std::move(mergingContext))
        , IntervalIdx(MergingContext->GetIntervalIdx())
    {
        for (auto&& s : Sources) {
            AFL_VERIFY(s.second->IsDataReady());
        }

    }
};

void TFetchingInterval::ConstructResult() {
    if (ReadySourcesCount.Val() != WaitSourcesCount || !ReadyGuards.Val()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_construct_result")("interval_idx", IntervalIdx);
        return;
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "start_construct_result")("interval_idx", IntervalIdx);
    }
    if (AtomicCas(&ResultConstructionInProgress, 1, 0)) {
        auto task = std::make_shared<TMergeTask>(std::move(MergingContext), Context, std::move(Sources));
        task->SetPriority(NConveyor::ITask::EPriority::High);
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }
}

void TFetchingInterval::OnInitResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "allocated")("interval_idx", IntervalIdx);
    AFL_VERIFY(guard);
    AFL_VERIFY(!ResourcesGuard);
    ResourcesGuard = guard;
    AFL_VERIFY(ReadyGuards.Inc() <= 1);
    ConstructResult();
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetched")("interval_idx", IntervalIdx);
    AFL_VERIFY(ReadySourcesCount.Inc() <= WaitSourcesCount);
    ConstructResult();
}

void TFetchingInterval::OnSourceFilterStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "filtered")("interval_idx", IntervalIdx);
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
    const bool includeFinish, const bool includeStart)
    : TTaskBase(0, context->GetMemoryForSources(sources, TMergingContext::IsExclusiveInterval(sources.size(), includeStart, includeFinish)), "", context->GetCommonContext()->GetResourcesTaskContext())
    , MergingContext(std::make_shared<TMergingContext>(start, finish, intervalIdx, includeFinish, includeStart))
    , Context(context)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , Sources(sources)
    , IntervalIdx(intervalIdx)
{
    Y_ABORT_UNLESS(Sources.size());
    for (auto&& [_, i] : Sources) {
        if (!i->IsDataReady()) {
            ++WaitSourcesCount;
        }
        i->RegisterInterval(this);
    }
}

void TFetchingInterval::DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_VERIFY(guard);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("interval_idx", IntervalIdx)("event", "resources_allocated")
        ("resources", guard->DebugString())("start", MergingContext->GetIncludeStart())("finish", MergingContext->GetIncludeFinish())("sources", Sources.size());
    for (auto&& [_, i] : Sources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(MergingContext->IsExclusiveInterval(Sources.size())), i);
    }
    OnInitResourcesGuard(guard);
}

}
