#include "scan_gate.h"

#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>

#include <ydb/library/actors/core/actor.h>

#include <algorithm>

namespace NKikimr::NOlap::NReader {

bool TScanGate::ArePortionsLoaded(const IColumnEngine* engine, const TScanTarget& target) {
    return !engine || target.TableMetadataAccessor->ArePortionsLoaded(*engine, *target.PKRangesFilter);
}

std::optional<ui64> TScanGate::ReleaseReadable(const TSnapshot& maxReadVersion) {
    while (!WaitingForSnapshot.empty()) {
        auto it = WaitingForSnapshot.begin();
        if (maxReadVersion < it->first) {
            return it->first.GetPlanStep();
        }
        TActivationContext::Send(it->second.Release());
        WaitingForSnapshot.erase(it);
    }
    return std::nullopt;
}

void TScanGate::ReleaseLoadedPortions(const IColumnEngine& engine) {
    const auto firstLoaded = std::stable_partition(WaitingForLoadedPortions.begin(), WaitingForLoadedPortions.end(), [&](const auto& waiting) {
        return !ArePortionsLoaded(&engine, waiting.Target);
    });
    for (auto it = firstLoaded; it != WaitingForLoadedPortions.end(); ++it) {
        TActivationContext::Send(it->Ev.Release());
    }
    WaitingForLoadedPortions.erase(firstLoaded, WaitingForLoadedPortions.end());
}

ui64 TScanGate::GetMemoryUsage() const {
    return WaitingForSnapshot.size() * (sizeof(TSnapshot) + sizeof(void*)) + WaitingForLoadedPortions.size() * sizeof(TWaitingForLoadedPortions);
}

}   // namespace NKikimr::NOlap::NReader
