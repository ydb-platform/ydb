#include "context.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

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
    const auto lockCategory = features.GetRebuildDecimalPrimaryKey() ? NDataLocks::ELockCategory::Any : NDataLocks::ELockCategory::Actualization;
    if (DataLocksManager->IsLocked(*info, lockCategory)) {
        return EAddPortionResult::PORTION_LOCKED;
    }

    const auto buildNewTask = [&]() {
        return TTaskConstructor(
            TTTLColumnEngineChanges::BuildMemoryPredictor(), std::make_shared<TTTLColumnEngineChanges>(features.GetRWAddress(), SaverContext));
    };
    auto it = Tasks.find(features.GetRWAddress());
    if (it == Tasks.end()) {
        if (!Controller->IsNewTaskAvailable(features.GetRWAddress(), 0)) {
            return EAddPortionResult::TASK_LIMIT_EXCEEDED;
        }
        std::vector<TTaskConstructor> tasks = { buildNewTask() };
        it = Tasks.emplace(features.GetRWAddress(), std::move(tasks)).first;
    }
    // An Any repair lock must not hide compatible portions from readers.
    TTaskConstructor* task = nullptr;
    for (auto candidate = it->second.rbegin(); candidate != it->second.rend(); ++candidate) {
        if (!candidate->GetPortionsCount() || candidate->GetTask()->IsKeyRebuild() == features.GetRebuildDecimalPrimaryKey()) {
            task = &*candidate;
            break;
        }
    }
    const bool txLimit = task && !task->CanTakePortionInTx(info, VersionedIndex);
    const bool memoryLimit = task && task->GetPortionsCount() && features.NeedRewrite() &&
                             task->GetMemoryPredictor()->PredictMemory(info, features) > MemoryUsageLimit;
    if (!task || txLimit || memoryLimit) {
        if (!Controller->IsNewTaskAvailable(it->first, it->second.size())) {
            return EAddPortionResult::TASK_LIMIT_EXCEEDED;
        }
        it->second.emplace_back(buildNewTask());
        task = &it->second.back();
        if (dWait && txLimit) {
            features.OnSkipPortionWithTxLimit(Counters, *dWait);
        }
        if (dWait && memoryLimit) {
            features.OnSkipPortionWithProcessMemory(Counters, *dWait);
        }
    }
    if (features.NeedRewrite()) {
        task->MutableMemoryUsage() = task->GetMemoryPredictor()->AddPortion(info, features);
    }
    task->TakePortionInTx(info, VersionedIndex);
    if (features.GetTargetTierName() == NTiering::NCommon::DeleteTierName) {
        AFL_VERIFY(dWait);
        Counters.OnPortionToDrop(info->GetTotalBlobBytes(), *dWait);
        task->GetTask()->AddPortionToRemove(info);
        AFL_VERIFY(!task->GetTask()->GetPortionsToEvictCount())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    } else {
        if (!dWait) {
            AFL_VERIFY(features.GetRebuildDecimalPrimaryKey() || features.GetCurrentScheme()->GetVersion() < features.GetTargetScheme()->GetVersion());
        } else {
            Counters.OnPortionToEvict(info->GetTotalBlobBytes(), *dWait);
        }
        task->GetTask()->AddPortionToEvict(info, std::move(features));
        AFL_VERIFY(!task->GetTask()->GetPortionsToRemove().HasPortions())("rw", features.GetRWAddress().DebugString())(
            "f", it->first.DebugString());
    }
    return EAddPortionResult::SUCCESS;
}

}   // namespace NKikimr::NOlap::NActualizer
