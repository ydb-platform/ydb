#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <util/system/rwlock.h>

#include <memory>

namespace NKikimr {

// Payloads can outlive both the scan and the actor system. Serialize broker
// dispatch against the shutdown hook so it completes before the pools stop.
class TScanReservationRuntime {
public:
    static void Install(NActors::TActorSystemSetup& setup);
    static std::shared_ptr<const TScanReservationRuntime> Get(NActors::TActorSystem& system);

    template<class F>
    void WithActorSystem(F&& fn) const {
        TReadGuard guard(Lock);
        if (System) {
            fn(*System);
        }
    }

private:
    class TSubSystem;
    mutable TRWMutex Lock;
    NActors::TActorSystem* System = nullptr;
};

class TScanReservationRuntime::TSubSystem final : public NActors::ISubSystem {
public:
    const std::shared_ptr<TScanReservationRuntime> Runtime = std::make_shared<TScanReservationRuntime>();

    void OnBeforeStart(NActors::TActorSystem& system) override {
        TWriteGuard guard(Runtime->Lock);
        Runtime->System = &system;
    }

    void OnBeforeStop(NActors::TActorSystem&) override {
        TWriteGuard guard(Runtime->Lock);
        Runtime->System = nullptr;
    }
};

inline void TScanReservationRuntime::Install(NActors::TActorSystemSetup& setup) {
    setup.RegisterSubSystem(std::make_unique<TSubSystem>());
}

inline std::shared_ptr<const TScanReservationRuntime> TScanReservationRuntime::Get(NActors::TActorSystem& system) {
    if (auto* subsystem = system.GetSubSystem<TSubSystem>()) {
        return subsystem->Runtime;
    }
    return {};
}

} // namespace NKikimr
