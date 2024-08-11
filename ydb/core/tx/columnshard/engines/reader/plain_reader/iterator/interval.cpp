#include "interval.h"

#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

void TFetchingInterval::ConstructResult() {
    if (ReadySourcesCount.Val() != WaitSourcesCount) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_construct_result")("interval_idx", IntervalIdx);
        return;
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "start_construct_result")("interval_idx", IntervalIdx);
    }
    if (AtomicCas(&SourcesFinalized, 1, 0)) {
        IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitMergerStart);

        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> guard;
        if (MergingContext->IsExclusiveInterval()) {
            AFL_VERIFY(Sources.size() == 1)("size", Sources.size());
            guard = Sources.begin()->second->GetResourcesGuard();
            MergingContext->SetIntervalChunkMemory(guard->GetMemory());
        } else {
            MergingContext->SetIntervalChunkMemory(Context->GetMemoryForSources(Sources));
        }

        auto task = std::make_shared<TStartMergeTask>(MergingContext, Context, std::move(Sources));
        task->SetPriority(NConveyor::ITask::EPriority::High);
        if (guard) {
            task->SetMemoryForAllocation(guard->GetMemory());
            task->OnAllocated(std::move(guard), task);
        } else {
            task->SetMemoryForAllocation(MergingContext->GetIntervalChunkMemory());
            NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation({ task }, GetIntervalId());
        }
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
    : MergingContext(std::make_shared<TMergingContext>(start, finish, intervalIdx, includeFinish, includeStart, isExclusiveInterval))
    , Context(context)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , Sources(sources)
    , IntervalIdx(intervalIdx)
    , IntervalGroupGuard(NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildGroupGuard())
    , IntervalStateGuard(Context->GetCommonContext()->GetCounters().CreateIntervalStateGuard()) {
    AFL_VERIFY(Sources.size());
    for (auto&& [_, i] : Sources) {
        if (!i->IsDataReady()) {
            ++WaitSourcesCount;
        }
        i->RegisterInterval(*this, i);
    }
    IntervalStateGuard.SetStatus(NColumnShard::TScanCounters::EIntervalStatus::WaitResources);
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
    task->SetMemoryForAllocation(MergingContext->GetIntervalChunkMemory());
    NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation({ task }, GetIntervalId());
}

}   // namespace NKikimr::NOlap::NReader::NPlain
