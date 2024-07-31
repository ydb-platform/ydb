#include "interval.h"
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

void TFetchingInterval::ConstructResult() {
    if (ReadySourcesCount.Val() != WaitSourcesCount || !ReadyGuards.Val()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_construct_result")("interval_idx", IntervalIdx);
        return;
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "start_construct_result")("interval_idx", IntervalIdx);
    }
    if (AtomicCas(&SourcesFinalized, 1, 0)) {
        IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitMergerStart);
        auto task = std::make_shared<TStartMergeTask>(MergingContext, Context, std::move(Sources));
        task->SetPriority(NConveyor::ITask::EPriority::High);
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetched")("interval_idx", IntervalIdx);
    AFL_VERIFY(ReadySourcesCount.Inc() <= WaitSourcesCount);
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const THashMap<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
    const bool includeFinish, const bool includeStart, const bool isExclusiveInterval)
    : TTaskBase(0, context->GetMemoryForSources(sources, isExclusiveInterval), "", context->GetCommonContext()->GetResourcesTaskContext())
    , MergingContext(std::make_shared<TMergingContext>(start, finish, intervalIdx, includeFinish, includeStart, isExclusiveInterval))
    , Context(context)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , Sources(sources)
    , ResourcesGuard(Context->GetCommonContext()->GetCounters().BuildRequestedResourcesGuard(GetMemoryAllocation()))
    , IntervalIdx(intervalIdx)
    , IntervalStateGuard(Context->GetCommonContext()->GetCounters().CreateIntervalStateGuard())
{
    Y_ABORT_UNLESS(Sources.size());
    for (auto&& [_, i] : Sources) {
        if (!i->IsDataReady()) {
            ++WaitSourcesCount;
        }
        i->RegisterInterval(*this);
    }
    IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitResources);
}

void TFetchingInterval::DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_VERIFY(guard);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("interval_idx", IntervalIdx)("event", "resources_allocated")
        ("resources", guard->DebugString())("start", MergingContext->GetIncludeStart())("finish", MergingContext->GetIncludeFinish())("sources", Sources.size());
    IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitSources);
    ResourcesGuard->InitResources(guard);
    for (auto&& i : Sources) {
        i.second->OnInitResourcesGuard(i.second);
    }
    AFL_VERIFY(ReadyGuards.Inc() <= 1);
    ConstructResult();
}

void TFetchingInterval::SetMerger(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger) {
    AFL_VERIFY(!Merger);
    AFL_VERIFY(AtomicCas(&PartSendingWait, 1, 0));
    if (merger) {
        IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitPartialReply);
    }
    Merger = std::move(merger);
}

bool TFetchingInterval::HasMerger() const {
    return !!Merger;
}

void TFetchingInterval::OnPartSendingComplete() {
    AFL_VERIFY(Merger);
    AFL_VERIFY(AtomicCas(&PartSendingWait, 0, 1));
    AFL_VERIFY(AtomicGet(SourcesFinalized) == 1);
    if (AbortedFlag) {
        return;
    }
    IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitMergerContinue);
    auto task = std::make_shared<TContinueMergeTask>(MergingContext, Context, std::move(Merger));
    task->SetPriority(NConveyor::ITask::EPriority::High);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

}
