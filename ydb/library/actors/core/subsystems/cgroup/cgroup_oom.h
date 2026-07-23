#pragma once

#include "cgroup_memory.h"
#include "../defs.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

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

    enum class ECGroupOomTrendWindow : ui8 {
        ShortWindow = 0,
        LongWindow = 1,
    };

    struct TCGroupOomTrendWindowConfig {
        // The fixed sample window used for linear regression.
        TDuration Duration;

        // Samples are collected on every poll, while regression and the OOM
        // forecast are updated no more often than this period.
        TDuration RecalculationPeriod = TDuration::Seconds(1);

        // A trend is available only when the coefficient of determination
        // reaches this value.
        double MinimumRSquared = 0.8;
    };

    struct TCGroupOomTrendWindowsConfig {
        std::optional<TCGroupOomTrendWindowConfig> ShortWindow;
        std::optional<TCGroupOomTrendWindowConfig> LongWindow;

        const TCGroupOomTrendWindowConfig* Find(ECGroupOomTrendWindow window) const {
            switch (window) {
                case ECGroupOomTrendWindow::ShortWindow:
                    return ShortWindow ? &*ShortWindow : nullptr;
                case ECGroupOomTrendWindow::LongWindow:
                    return LongWindow ? &*LongWindow : nullptr;
            }
            return nullptr;
        }
    };

    struct TCGroupOomTrend {
        TCGroupMemoryStatsPtr Stats;
        ECGroupOomTrendWindow Window;
        TDuration WindowDuration;
        double GrowthBytesPerSecond = 0;
        double RSquared = 0;
        TDuration TimeToOom;
        ui64 SampleCount = 0;
    };

    enum class ECGroupOomTrendState {
        Active,
        Stopped,
    };

    struct TCGroupOomConfig {
        // The monitor actor runs in this pool. Actual cgroup file reads use the
        // pools configured by the selected stats providers.
        ui32 ExecutorPoolId = 0;
        TDuration PollPeriod = TDuration::Seconds(1);

        // The cgroup version and path are fixed by the first memory snapshot.
        // Moving a running process between cgroups stops OOM monitoring; restart
        // the process in the target cgroup instead.

        // An empty optional disables the corresponding threshold.
        std::optional<double> MemoryUsageThreshold = 0.9;
        std::optional<ui64> AvailableBytesThreshold;
        std::optional<double> MemoryPressureFullAvg10Threshold;

        // Trend windows are fixed for the lifetime of the subsystem. Each
        // window is disabled when the corresponding optional is empty.
        TCGroupOomTrendWindowsConfig TrendWindows;
    };

    class TCGroupOomSubSystem : public ISubSystem {
    public:
        explicit TCGroupOomSubSystem(TCGroupOomConfig config);

        const TCGroupOomConfig& GetConfig() const {
            return Config;
        }

        TSubSystemDependencies GetDependencies() const override;
        void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override;

        // Actor subscriptions are local and keyed by ActorId. Repeated
        // Subscribe and Unsubscribe calls are idempotent. Subscription changes
        // are applied asynchronously by the monitor actor.
        void Subscribe(const TActorId& actorId);
        void Unsubscribe(const TActorId& actorId);

        // Sends TEvCGroupOomTrend after the first calculation for the window.
        // The request remains pending while the window is accumulating. Once
        // calculated, the response is sent even when no trend was found.
        void ReadTrend(
            const TActorId& recipient,
            ECGroupOomTrendWindow window,
            ui64 cookie = 0) const;

        // Repeated actor subscriptions with the same window and threshold are
        // idempotent. Active is delivered after every window recalculation
        // while the threshold is met; Stopped is delivered once when it ceases
        // to hold. UnsubscribeFromTrend removes all trend subscriptions for the
        // actor.
        void SubscribeToTrend(
            const TActorId& actorId,
            ECGroupOomTrendWindow window,
            TDuration timeToOomThreshold);
        void UnsubscribeFromTrend(const TActorId& actorId);

        void OnAfterStart(TActorSystem& actorSystem) override;
        void OnBeforeStop(TActorSystem& actorSystem) override;

    private:
        void ValidateTrendWindow(ECGroupOomTrendWindow window) const;
        void ValidateTrendSubscription(
            ECGroupOomTrendWindow window,
            TDuration timeToOomThreshold) const;

    private:
        const TCGroupOomConfig Config;
        TVector<const ICGroupMemoryStatsProvider*> Providers;

        TActorSystem* ActorSystem = nullptr;
        TActorId MonitorActorId;
    };

    std::unique_ptr<TCGroupOomSubSystem> MakeCGroupOomSubSystem(TCGroupOomConfig config);

    TCGroupOomSubSystem& GetCGroupOomSubSystem(TActorSystem& actorSystem);
    TCGroupOomSubSystem& GetCGroupOomSubSystem();

} // namespace NActors
