#include "context.h"
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap::NActualizer {

TTieringProcessContext::TTieringProcessContext(const ui64 memoryUsageLimit, const TSaverContext& saverContext,
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const TVersionedIndex& versionedIndex,
    const NColumnShard::TEngineLogsCounters& counters, const std::shared_ptr<TController>& controller)
    : VersionedIndex(versionedIndex)
    , MemoryUsageLimit(memoryUsageLimit)
    , SaverContext(saverContext)
    , Counters(counters)
    , Controller(controller)
    , ActualInstant(TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now())
    , DataLocksManager(dataLocksManager)
{

}

TTieringProcessContext::EAddPortionResult TTieringProcessContext::AddPortion(
    const std::shared_ptr<const TPortionInfo>& info, TPortionEvictionFeatures&& features, const std::optional<TDuration> dWait) {
    if (!UsedPortions.emplace(info->GetAddress()).second) {
        return EAddPortionResult::PORTION_LOCKED;
    }
    if (DataLocksManager->IsLocked(*info, NDataLocks::ELockCategory::Actualization)) {
        return EAddPortionResult::PORTION_LOCKED;
    }

    const auto buildNewTask = [&]() {
        return TTaskConstructor(TTTLColumnEngineChanges::BuildMemoryPredictor(), std::make_shared<TTTLColumnEngineChanges>(features.GetRWAddress(), SaverContext));
    };
    auto it = Tasks.find(features.GetRWAddress());
    if (it == Tasks.end()) {
        std::vector<TTaskConstructor> tasks = { buildNewTask() };
        it = Tasks.emplace(features.GetRWAddress(), std::move(tasks)).first;
    }
    if (!it->second.back().CanTakePortionInTx(info, VersionedIndex)) {
        if (Controller->IsNewTaskAvailable(it->first, it->second.size())) {
            it->second.emplace_back(buildNewTask());
        } else {
            return EAddPortionResult::TASK_LIMIT_EXCEEDED;
        }
        features.OnSkipPortionWithProcessMemory(Counters, *dWait);
    }
    if (features.NeedRewrite()) {
        if (MemoryUsageLimit <= it->second.back().GetMemoryUsage()) {
            if (Controller->IsNewTaskAvailable(it->first, it->second.size())) {
                it->second.emplace_back(buildNewTask());
            } else {
                return EAddPortionResult::TASK_LIMIT_EXCEEDED;
            }
            features.OnSkipPortionWithTxLimit(Counters, *dWait);
        }
        it->second.back().MutableMemoryUsage() = it->second.back().GetMemoryPredictor()->AddPortion(info);
    }
    it->second.back().TakePortionInTx(info, VersionedIndex);
    if (features.GetTargetTierName() == NTiering::NCommon::DeleteTierName) {
        AFL_VERIFY(dWait);
        Counters.OnPortionToDrop(info->GetTotalBlobBytes(), *dWait);
        it->second.back().GetTask()->AddPortionToRemove(info);
        AFL_VERIFY(!it->second.back().GetTask()->GetPortionsToEvictCount())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    } else {
        if (!dWait) {
            AFL_VERIFY(features.GetCurrentScheme()->GetVersion() < features.GetTargetScheme()->GetVersion());
        } else {
            Counters.OnPortionToEvict(info->GetTotalBlobBytes(), *dWait);
        }
        it->second.back().GetTask()->AddPortionToEvict(info, std::move(features));
        AFL_VERIFY(!it->second.back().GetTask()->GetPortionsToRemove().HasPortions())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    }
    return EAddPortionResult::SUCCESS;
}

}