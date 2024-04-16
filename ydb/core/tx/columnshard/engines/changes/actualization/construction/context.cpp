#include "context.h"
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

TTieringProcessContext::TTieringProcessContext(const ui64 memoryUsageLimit, const TSaverContext& saverContext,
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const NColumnShard::TEngineLogsCounters& counters, const std::shared_ptr<TController>& controller)
    : MemoryUsageLimit(memoryUsageLimit)
    , SaverContext(saverContext)
    , Counters(counters)
    , Controller(controller)
    , DataLocksManager(dataLocksManager)
    , Now(TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now())
{

}

bool TTieringProcessContext::AddPortion(const TPortionInfo& info, TPortionEvictionFeatures&& features, const std::optional<TDuration> dWait, const TInstant now) {
    const TInstant maxChangePortionInstant = info.RecordSnapshotMax().GetPlanInstant();
    if (features.GetTargetTierName() != IStoragesManager::DefaultStorageId && info.GetTierNameDef(IStoragesManager::DefaultStorageId) == IStoragesManager::DefaultStorageId) {
        if (now - maxChangePortionInstant < NYDBTest::TControllers::GetColumnShardController()->GetLagForCompactionBeforeTierings(TDuration::Minutes(60))) {
            Counters.OnActualizationSkipTooFreshPortion(now - maxChangePortionInstant);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_portion_to_evict")("reason", "too_fresh")("delta", now - maxChangePortionInstant);
            return true;
        }
    }

    const auto buildNewTask = [&]() {
        return TTaskConstructor(TTTLColumnEngineChanges::BuildMemoryPredictor(), std::make_shared<TTTLColumnEngineChanges>(features.GetRWAddress(), TSplitSettings(), SaverContext));
    };
    auto it = Tasks.find(features.GetRWAddress());
    if (it == Tasks.end()) {
        std::vector<TTaskConstructor> tasks = {buildNewTask()};
        it = Tasks.emplace(features.GetRWAddress(), std::move(tasks)).first;
    }
    if (it->second.back().GetTxWriteVolume() + info.GetTxVolume() > TGlobalLimits::TxWriteLimitBytes && it->second.back().GetTxWriteVolume()) {
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
        Counters.OnPortionToDrop(info.GetBlobBytes(), *dWait);
        it->second.back().GetTask()->PortionsToRemove.emplace(info.GetAddress(), info);
        AFL_VERIFY(!it->second.back().GetTask()->GetPortionsToEvictCount())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    } else {
        AFL_VERIFY(dWait);
        Counters.OnPortionToEvict(info.GetBlobBytes(), *dWait);
        it->second.back().GetTask()->AddPortionToEvict(info, std::move(features));
        AFL_VERIFY(it->second.back().GetTask()->PortionsToRemove.empty())("rw", features.GetRWAddress().DebugString())("f", it->first.DebugString());
    }
    return true;
}

}