#include "interconnect_common.h"
#include "interconnect_uring_engine.h"

namespace NActors {

    TInterconnectProxyCommon::TInterconnectProxyCommon() = default;
    TInterconnectProxyCommon::~TInterconnectProxyCommon() = default;

    void TInterconnectProxyCommon::SetUringEngineV2(TIntrusivePtr<IUringEngine> engine) {
        TGuard<TMutex> guard(UringEngineLock);
        UringEngineV2 = std::move(engine);
    }

    TIntrusivePtr<IUringEngine> TInterconnectProxyCommon::GetUringEngineV2() const {
        TGuard<TMutex> guard(UringEngineLock);
        return UringEngineV2;
    }

    TIntrusivePtr<IUringEngine> TInterconnectProxyCommon::EnsureUringEngineV2(TActorSystem* actorSystem, ui32 numShards,
            NMonitoring::TDynamicCounterPtr counters) {
        TGuard<TMutex> guard(UringEngineLock);
        if (!UringEngineV2) {
            UringEngineV2 = CreateUringEngine(actorSystem, numShards, counters);
            if (UringEngineV2 && actorSystem) {
                // Stop the reaper threads while the actor system is still up, so no completion is posted
                // to a torn-down system.
                actorSystem->DeferPreStop([engine = UringEngineV2] { engine->Stop(); });
            }
        }
        return UringEngineV2;
    }

    double TInterconnectProxyCommon::CalculateNetworkUtilization() {
        const ui64 sessions = NumSessionsWithDataInQueue.load();
        const ui64 ts = GetCycleCountFast();
        const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
        const ui64 passed = ts - prevts;
        const ui64 zero = CyclesWithZeroSessions.exchange(0) + (sessions ? 0 : passed);
        const ui64 nonzero = CyclesWithNonzeroSessions.exchange(0) + (sessions ? passed : 0);
        return (double)nonzero / (zero + nonzero);
    }

    void TInterconnectProxyCommon::AddSessionWithDataInQueue() {
        if (!NumSessionsWithDataInQueue++) {
            const ui64 ts = GetCycleCountFast();
            const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
            CyclesWithZeroSessions += ts - prevts;
        }
    }

    void TInterconnectProxyCommon::RemoveSessionWithDataInQueue() {
        if (!--NumSessionsWithDataInQueue) {
            const ui64 ts = GetCycleCountFast();
            const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
            CyclesWithNonzeroSessions += ts - prevts;
        }
    }

}
