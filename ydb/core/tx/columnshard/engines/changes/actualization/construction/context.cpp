#include "context.h"
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap::NActualizer {

TTieringProcessContext::TTieringProcessContext(const ui64 memoryUsageLimit, const TSaverContext& saverContext,
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const NColumnShard::TEngineLogsCounters& counters, const std::shared_ptr<TController>& controller)
    : MemoryUsageLimit(memoryUsageLimit)
    , SaverContext(saverContext)
    , Counters(counters)
    , Controller(controller)
    , ActualInstant(TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now())
    , DataLocksManager(dataLocksManager)
{

}

bool TTieringProcessContext::AddPortion(const TPortionInfo& info, TPortionEvictionFeatures&& features, const std::optional<TDuration> dWait) {
    if (!UsedPortions.emplace(info.GetAddress()).second) {
        return true;
    }
    if (DataLocksManager->IsLocked(info)) {
        return true;
    }

    const auto buildNewTask = [&]() {
        return TTaskConstructor(TTTLColumnEngineChanges::BuildMemoryPredictor(), std::make_shared<TTTLColumnEngineChanges>(features.GetRWAddress(), SaverContext));
    };
    auto it = Tasks.find(features.GetRWAddress());
    if (it == Tasks.end()) {
        std::vector<TTaskConstructor> tasks = {buildNewTask()};
        it = Tasks.emplace(features.GetRWAddress(), std::move(tasks)).first;
    }
    if (it->second.back().GetTxWriteVolume() + info.GetTxVolume() > TGlobalLimits::TxWriteLimitBytes / 2 && it->second.back().GetTxWriteVolume()) {
        if (Controller->IsNewTaskAvailable(it->first, it->second.size())) {
            it->second.emplace_back(buildNewTask());
        } else {
            return false;
        }
        features.OnSkipPortionWithProcessMemory(Counters, *dWait);
    }
    if (features.NeedRewrite()) {
        if (MemoryUsageLimit <= it->second.back().GetMemoryUsage()) {
            if (Controller->IsNewTaskAvailable(it->first, it->second.size())) {
                it->second.emplace_back(buildNewTask());
            } else {
                return false;
            }
            features.OnSkipPortionWithTxLimit(Counters, *dWait);
        }
        it->second.back().MutableMemoryUsage() = it->second.back().GetMemoryPredictor()->AddPortion(info);
    }
    it->second.back().MutableTxWriteVolume() += info.GetTxVolume();
    if (features.GetTargetTierName() == NTiering::NCommon::DeleteTierName) {
        AFL_VERIFY(dWait);
        Counters.OnPortionToDrop(info.GetTotalBlobBytes(), *dWait);
        it->second.back().GetTask()->AddPortionToRemove(info);
        AFL_VERIFY(!it->second.back().GetTask()->GetPortionsToEvictCount())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    } else {
        if (!dWait) {
            AFL_VERIFY(features.GetCurrentScheme()->GetVersion() < features.GetTargetScheme()->GetVersion());
        } else {
            Counters.OnPortionToEvict(info.GetTotalBlobBytes(), *dWait);
        }
        it->second.back().GetTask()->AddPortionToEvict(info, std::move(features));
        AFL_VERIFY(!it->second.back().GetTask()->HasPortionsToRemove())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    }
    return true;
}

}