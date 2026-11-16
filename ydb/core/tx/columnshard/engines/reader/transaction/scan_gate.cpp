#include "scan_gate.h"

#include <ydb/core/tx/columnshard/counters/tablet_counters.h>
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <ydb/library/actors/core/actor.h>

#include <algorithm>

namespace NKikimr::NOlap::NReader {

bool TScanGate::ArePortionsLoaded(const TScanTarget& target) const {
    const auto& engine = TablesManager.GetPrimaryIndex();
    return !engine || target.TableMetadataAccessor->ArePortionsLoaded(*engine, *target.PKRangesFilter);
}

void TScanGate::UpdateCounters() const {
    TabletCounters->SetCounter(NColumnShard::COUNTER_SCANS_WAITING_SNAPSHOT, WaitingForSnapshot.size());
    TabletCounters->SetCounter(NColumnShard::COUNTER_SCANS_WAITING_LOADED_PORTIONS, WaitingForLoadedPortions.size());
}

std::optional<ui64> TScanGate::ReleaseReadableSnapshot(const TSnapshot& maxReadVersion) {
    std::optional<ui64> waitedStep;
    while (!WaitingForSnapshot.empty()) {
        auto it = WaitingForSnapshot.begin();
        if (maxReadVersion < it->first) {
            waitedStep = it->first.GetPlanStep();
            break;
        }
        TActivationContext::Send(it->second.Release());
        WaitingForSnapshot.erase(it);
    }
    UpdateCounters();
    return waitedStep;
}

void TScanGate::ReleaseLoadedPortions() {
    const auto firstLoaded = std::stable_partition(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [&](const auto& waiting) {
        return !ArePortionsLoaded(waiting.Target);
    });
    for (auto it = firstLoaded; it != WaitingForLoadedPortions.end(); ++it) {
        TActivationContext::Send(it->Ev.Release());
    }
    WaitingForLoadedPortions.erase(firstLoaded, WaitingForLoadedPortions.end());
    UpdateCounters();
}

ui64 TScanGate::GetMemoryUsage() const {
    return WaitingForSnapshot.size() * (sizeof(TSnapshot) + sizeof(void*)) + WaitingForLoadedPortions.size() * sizeof(TWaitingForLoadedPortions);
}

}   // namespace NKikimr::NOlap::NReader
