#pragma once

#include "cgroup_memory.h"
#include "../defs.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>

namespace NActors {

    enum class ECGroupOomReason : ui32 {
        // CurrentBytes / MaxBytes reached or exceeded
        // Config.MemoryUsageThreshold.
        MemoryUsageThreshold = 1 << 0,

        // MaxBytes - CurrentBytes fell to or below
        // Config.AvailableBytesThreshold.
        AvailableBytesThreshold = 1 << 1,

        // cgroup v2 memory.pressure "full avg10" reached or exceeded
        // Config.MemoryPressureFullAvg10Threshold.
        MemoryPressureThreshold = 1 << 2,

        // The cgroup v2 memory.events "high" counter increased since the
        // previous snapshot.
        MemoryHighEvent = 1 << 3,

        // The hard-limit counter increased since the previous snapshot:
        // cgroup v2 memory.events "max" or cgroup v1 memory.failcnt.
        MemoryMaxEvent = 1 << 4,
    };

    struct TCGroupOomAlert {
        TCGroupMemoryStatsPtr Stats;
        ui32 Reasons = 0;

        std::optional<double> MemoryUsage;
        std::optional<ui64> AvailableBytes;

        ui64 NewHighEvents = 0;
        ui64 NewMaxEvents = 0;

        bool HasReason(ECGroupOomReason reason) const {
            return Reasons & static_cast<ui32>(reason);
        }
    };

    struct TCGroupOomConfig {
        // The monitor actor and callbacks run in this pool. Actual cgroup file
        // reads use the pools configured by the selected stats providers.
        ui32 ExecutorPoolId = 0;
        TDuration PollPeriod = TDuration::Seconds(1);

        // An empty optional disables the corresponding threshold.
        std::optional<double> MemoryUsageThreshold = 0.9;
        std::optional<ui64> AvailableBytesThreshold;
        std::optional<double> MemoryPressureFullAvg10Threshold;
    };

    class TCGroupOomSubSystem : public ISubSystem {
    public:
        using TSubscriptionId = ui64;
        using TCallback = std::function<void(const TCGroupOomAlert&)>;

        explicit TCGroupOomSubSystem(TCGroupOomConfig config);

        const TCGroupOomConfig& GetConfig() const {
            return Config;
        }

        TSubSystemDependencies GetDependencies() const override;
        void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override;

        // Subscription changes are applied asynchronously by the monitor
        // actor. The actor system must be running when these methods are
        // called. A notification already ahead of Unsubscribe in the actor's
        // mailbox may still be delivered. Threshold notifications are
        // edge-triggered; counter deltas trigger notifications whenever they
        // increase.
        TSubscriptionId Subscribe(TCallback callback);
        void Unsubscribe(TSubscriptionId subscriptionId);

        // Actor subscriptions are local and keyed by ActorId. Repeated
        // Subscribe and Unsubscribe calls are idempotent.
        void Subscribe(const TActorId& actorId);
        void Unsubscribe(const TActorId& actorId);

        void OnAfterStart(TActorSystem& actorSystem) override;
        void OnBeforeStop(TActorSystem& actorSystem) override;

    private:
        const TCGroupOomConfig Config;
        TVector<const ICGroupMemoryStatsProvider*> Providers;

        TActorSystem* ActorSystem = nullptr;
        TActorId MonitorActorId;
        alignas(64) std::atomic<TSubscriptionId> NextSubscriptionId = 1;
    };

    std::unique_ptr<TCGroupOomSubSystem> MakeCGroupOomSubSystem(TCGroupOomConfig config);

    TCGroupOomSubSystem& GetCGroupOomSubSystem(TActorSystem& actorSystem);
    TCGroupOomSubSystem& GetCGroupOomSubSystem();

} // namespace NActors
