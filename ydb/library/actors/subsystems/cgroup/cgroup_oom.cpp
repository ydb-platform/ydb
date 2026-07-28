#include "cgroup_oom.h"

#include "cgroup_events.h"
#include "cgroup_oom_trend.h"
#include "cgroup_v1.h"
#include "cgroup_v2.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <algorithm>
#include <array>

namespace NActors {

    namespace {

        ui32 ReasonMask(ECGroupOomReason reason) {
            return static_cast<ui32>(reason);
        }

        ui64 CounterDelta(ui64 current, ui64 previous) {
            return current >= previous ? current - previous : 0;
        }

    } // namespace

    namespace NDetail {

        enum : ui32 {
            EvPoll = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvSubscribeActor,
            EvUnsubscribeActor,
            EvReadTrend,
            EvSubscribeTrendActor,
            EvUnsubscribeTrendActor,
        };

        struct TEvPoll : TEventLocal<TEvPoll, EvPoll> {
        };

        struct TEvSubscribeActor : TEventLocal<TEvSubscribeActor, EvSubscribeActor> {
            TActorId ActorId;

            explicit TEvSubscribeActor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

        struct TEvUnsubscribeActor : TEventLocal<TEvUnsubscribeActor, EvUnsubscribeActor> {
            TActorId ActorId;

            explicit TEvUnsubscribeActor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

        struct TEvReadTrend : TEventLocal<TEvReadTrend, EvReadTrend> {
            TActorId Recipient;
            ECGroupOomTrendWindow Window;
            ui64 Cookie;

            TEvReadTrend(
                    TActorId recipient,
                    ECGroupOomTrendWindow window,
                    ui64 cookie)
                : Recipient(recipient)
                , Window(window)
                , Cookie(cookie)
            {
            }
        };

        struct TEvSubscribeTrendActor
            : TEventLocal<TEvSubscribeTrendActor, EvSubscribeTrendActor>
        {
            TActorId ActorId;
            ECGroupOomTrendWindow Window;
            TDuration TimeToOomThreshold;

            TEvSubscribeTrendActor(
                    TActorId actorId,
                    ECGroupOomTrendWindow window,
                    TDuration timeToOomThreshold)
                : ActorId(actorId)
                , Window(window)
                , TimeToOomThreshold(timeToOomThreshold)
            {
            }
        };

        struct TEvUnsubscribeTrendActor
            : TEventLocal<TEvUnsubscribeTrendActor, EvUnsubscribeTrendActor>
        {
            TActorId ActorId;

            explicit TEvUnsubscribeTrendActor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

        class TCGroupOomActor : public TActorBootstrapped<TCGroupOomActor> {
        public:
            TCGroupOomActor(
                    TVector<const ICGroupMemoryStatsProvider*> providers,
                    TCGroupOomConfig config)
                : Providers(std::move(providers))
                , Config(std::move(config))
                , TrendCalculator(Config.TrendWindows)
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
                Send(SelfId(), new TEvPoll());
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvPoll, Handle)
                hFunc(TEvCGroupMemoryStats, Handle)
                hFunc(TEvSubscribeActor, Handle)
                hFunc(TEvUnsubscribeActor, Handle)
                hFunc(TEvReadTrend, Handle)
                hFunc(TEvSubscribeTrendActor, Handle)
                hFunc(TEvUnsubscribeTrendActor, Handle)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )

        private:
            struct TTrendSubscription {
                TDuration TimeToOomThreshold;
                bool Active = false;

                explicit TTrendSubscription(TDuration timeToOomThreshold)
                    : TimeToOomThreshold(timeToOomThreshold)
                {
                }
            };

            struct TPendingTrendRequest {
                TActorId Recipient;
                ui64 Cookie;
            };

            struct TTrendActorSubscription : TTrendSubscription {
                TActorId ActorId;

                TTrendActorSubscription(
                        TActorId actorId,
                        TDuration timeToOomThreshold)
                    : TTrendSubscription(timeToOomThreshold)
                    , ActorId(actorId)
                {
                }
            };

            void Handle(TEvPoll::TPtr&) {
                RequestProvider(0);
            }

            void RequestProvider(size_t providerIndex) {
                Providers[providerIndex]->ReadMemoryStats(SelfId(), providerIndex);
            }

            void Handle(TEvCGroupMemoryStats::TPtr& ev) {
                Y_ABORT_UNLESS(ev->Cookie < Providers.size(),
                    "invalid cgroup memory stats provider index: %" PRIu64,
                    ev->Cookie);
                const size_t providerIndex = ev->Cookie;

                if (ev->Get()->Stats) {
                    if (!Evaluate(std::move(ev->Get()->Stats))) {
                        return;
                    }
                    Schedule(Config.PollPeriod, new TEvPoll());
                } else if (providerIndex + 1 < Providers.size()) {
                    RequestProvider(providerIndex + 1);
                } else {
                    Schedule(Config.PollPeriod, new TEvPoll());
                }
            }

            void Handle(TEvSubscribeActor::TPtr& ev) {
                ActorSubscribers.insert(ev->Get()->ActorId);
            }

            void Handle(TEvUnsubscribeActor::TPtr& ev) {
                ActorSubscribers.erase(ev->Get()->ActorId);
            }

            void Handle(TEvReadTrend::TPtr& ev) {
                if (TrendCalculator.HasResult(ev->Get()->Window)) {
                    const auto trend = GetTrend(ev->Get()->Window);
                    SendTrend(
                        ev->Get()->Recipient,
                        trend
                            ? ECGroupOomTrendState::Active
                            : ECGroupOomTrendState::Stopped,
                        trend,
                        std::nullopt,
                        ev->Get()->Cookie);
                } else {
                    PendingTrendRequests[CGroupOomTrendWindowIndex(
                        ev->Get()->Window)].push_back({
                        .Recipient = ev->Get()->Recipient,
                        .Cookie = ev->Get()->Cookie,
                    });
                }
            }

            void Handle(TEvSubscribeTrendActor::TPtr& ev) {
                const auto* event = ev->Get();
                auto& subscriptions =
                    TrendActors[CGroupOomTrendWindowIndex(event->Window)];
                const auto existing = std::find_if(
                    subscriptions.begin(),
                    subscriptions.end(),
                    [event](const TTrendActorSubscription& subscription) {
                        return subscription.ActorId == event->ActorId &&
                            subscription.TimeToOomThreshold == event->TimeToOomThreshold;
                    });
                if (existing == subscriptions.end()) {
                    subscriptions.push_back({
                        event->ActorId,
                        event->TimeToOomThreshold,
                    });
                    if (TrendCalculator.HasResult(event->Window)) {
                        const auto trend = GetTrend(event->Window);
                        UpdateTrendActorSubscription(
                            trend,
                            subscriptions.back());
                    }
                }
            }

            void Handle(TEvUnsubscribeTrendActor::TPtr& ev) {
                const TActorId actorId = ev->Get()->ActorId;
                for (auto& subscriptions : TrendActors) {
                    subscriptions.erase(
                        std::remove_if(
                            subscriptions.begin(),
                            subscriptions.end(),
                            [actorId](const TTrendActorSubscription& subscription) {
                                return subscription.ActorId == actorId;
                            }),
                        subscriptions.end());
                }
            }

            bool Evaluate(TCGroupMemoryStatsPtr stats) {
                if (!CGroupVersion) {
                    CGroupVersion = stats->Version;
                    CGroupPath = stats->CGroupPath;
                } else if (*CGroupVersion != stats->Version ||
                        CGroupPath != stats->CGroupPath) {
                    Cerr
                        << "ERROR: cgroup memory stats source changed from version "
                        << static_cast<ui32>(*CGroupVersion)
                        << " path " << CGroupPath
                        << " to version " << static_cast<ui32>(stats->Version)
                        << " path " << stats->CGroupPath << Endl;
                    Cerr
                        << "Stopping cgroup OOM monitoring; restart the process "
                        << "in the target cgroup" << Endl;
                    PassAway();
                    return false;
                }

                const auto recalculated = TrendCalculator.AddSample(
                    TActivationContext::Monotonic(),
                    stats);

                TCGroupOomAlert alert;
                alert.Stats = std::move(stats);

                ui32 levelReasons = 0;
                const auto& memory = *alert.Stats;
                if (memory.MaxBytes && !memory.MaxBytes->Unlimited && memory.MaxBytes->Value) {
                    const ui64 maxBytes = memory.MaxBytes->Value;
                    alert.MemoryUsage = static_cast<double>(memory.CurrentBytes) / maxBytes;
                    alert.AvailableBytes = memory.CurrentBytes < maxBytes
                        ? maxBytes - memory.CurrentBytes
                        : 0;

                    if (Config.MemoryUsageThreshold && *alert.MemoryUsage >= *Config.MemoryUsageThreshold) {
                        levelReasons |= ReasonMask(ECGroupOomReason::MemoryUsageThreshold);
                    }
                    if (Config.AvailableBytesThreshold && *alert.AvailableBytes <= *Config.AvailableBytesThreshold) {
                        levelReasons |= ReasonMask(ECGroupOomReason::AvailableBytesThreshold);
                    }
                }

                if (Config.MemoryPressureFullAvg10Threshold && memory.PressureFullAvg10 &&
                        *memory.PressureFullAvg10 >= *Config.MemoryPressureFullAvg10Threshold) {
                    levelReasons |= ReasonMask(ECGroupOomReason::MemoryPressureThreshold);
                }
                alert.Reasons = levelReasons & ~ActiveLevelReasons;
                ActiveLevelReasons = levelReasons;

                if (HasPreviousMemory) {
                    alert.NewHighEvents = CounterDelta(memory.HighEvents, PreviousMemory.HighEvents);
                    alert.NewMaxEvents = CounterDelta(memory.MaxEvents, PreviousMemory.MaxEvents);

                    if (alert.NewHighEvents) {
                        alert.Reasons |= ReasonMask(ECGroupOomReason::MemoryHighEvent);
                    }
                    if (alert.NewMaxEvents) {
                        alert.Reasons |= ReasonMask(ECGroupOomReason::MemoryMaxEvent);
                    }
                }
                PreviousMemory = memory;
                HasPreviousMemory = true;

                if (alert.Reasons) {
                    Notify(alert);
                }
                for (ECGroupOomTrendWindow window : CGroupOomTrendWindows) {
                    if (recalculated.Contains(window)) {
                        ReplyToPendingTrendRequests(window);
                        UpdateTrendSubscriptions(window);
                    }
                }
                return true;
            }

            void Notify(const TCGroupOomAlert& alert) {
                const TActorContext& actorContext = TActivationContext::AsActorContext();
                for (const TActorId& actorId : ActorSubscribers) {
                    actorContext.Send(actorId, new TEvCGroupOomAlert(alert));
                }
            }

            void UpdateTrendSubscriptions(ECGroupOomTrendWindow window) {
                const auto windowIndex = CGroupOomTrendWindowIndex(window);
                const auto trend = GetTrend(window);

                for (auto& subscription : TrendActors[windowIndex]) {
                    UpdateTrendActorSubscription(trend, subscription);
                }
            }

            void UpdateTrendActorSubscription(
                    const std::optional<TCGroupOomTrend>& trend,
                    TTrendActorSubscription& subscription) {
                const bool active =
                    trend && trend->TimeToOom <= subscription.TimeToOomThreshold;
                if (!active && !subscription.Active) {
                    return;
                }

                SendTrend(
                    subscription.ActorId,
                    active
                        ? ECGroupOomTrendState::Active
                        : ECGroupOomTrendState::Stopped,
                    trend,
                    subscription.TimeToOomThreshold);
                subscription.Active = active;
            }

            std::optional<TCGroupOomTrend> GetTrend(
                    ECGroupOomTrendWindow window) const {
                if (const auto* trend = TrendCalculator.FindTrend(window)) {
                    return *trend;
                }
                return std::nullopt;
            }

            void SendTrend(
                    const TActorId& recipient,
                    ECGroupOomTrendState state,
                    const std::optional<TCGroupOomTrend>& trend,
                    std::optional<TDuration> timeToOomThreshold = std::nullopt,
                    ui64 cookie = 0) {
                Send(
                    recipient,
                    new TEvCGroupOomTrend(trend, state, timeToOomThreshold),
                    0,
                    cookie);
            }

            void ReplyToPendingTrendRequests(ECGroupOomTrendWindow window) {
                auto& requests =
                    PendingTrendRequests[CGroupOomTrendWindowIndex(window)];
                if (!TrendCalculator.HasResult(window)) {
                    return;
                }
                const auto trend = GetTrend(window);
                const auto state = trend
                    ? ECGroupOomTrendState::Active
                    : ECGroupOomTrendState::Stopped;
                for (const auto& request : requests) {
                    SendTrend(
                        request.Recipient,
                        state,
                        trend,
                        std::nullopt,
                        request.Cookie);
                }
                requests.clear();
            }

        private:
            const TVector<const ICGroupMemoryStatsProvider*> Providers;
            const TCGroupOomConfig Config;
            TCGroupOomTrendCalculator TrendCalculator;

            THashSet<TActorId> ActorSubscribers;
            std::array<
                TVector<TTrendActorSubscription>,
                CGroupOomTrendWindows.size()> TrendActors;
            std::array<
                TVector<TPendingTrendRequest>,
                CGroupOomTrendWindows.size()> PendingTrendRequests;

            std::optional<ECGroupVersion> CGroupVersion;
            TString CGroupPath;
            TCGroupMemoryStats PreviousMemory;
            ui32 ActiveLevelReasons = 0;
            bool HasPreviousMemory = false;
        };

    } // namespace NDetail

    TCGroupOomSubSystem::TCGroupOomSubSystem(TCGroupOomConfig config)
        : Config(std::move(config))
    {
        Y_ABORT_UNLESS(Config.PollPeriod > TDuration::Zero(),
            "cgroup OOM poll period must be positive");
        Y_ABORT_UNLESS(!Config.MemoryUsageThreshold ||
            (*Config.MemoryUsageThreshold > 0 && *Config.MemoryUsageThreshold <= 1),
            "cgroup OOM memory usage threshold must be in (0, 1]");
        Y_ABORT_UNLESS(!Config.MemoryPressureFullAvg10Threshold ||
            (*Config.MemoryPressureFullAvg10Threshold >= 0 &&
                *Config.MemoryPressureFullAvg10Threshold <= 100),
            "cgroup OOM PSI threshold must be in [0, 100]");

        const auto validateTrendWindow = [this](
                const std::optional<TCGroupOomTrendWindowConfig>& window) {
            if (!window) {
                return;
            }
            Y_ABORT_UNLESS(window->Duration >= Config.PollPeriod,
                "cgroup OOM trend window must be at least one poll period");
            Y_ABORT_UNLESS(
                window->RecalculationPeriod >= Config.PollPeriod &&
                    window->RecalculationPeriod <= window->Duration,
                "cgroup OOM trend recalculation period must be between the poll period and window");
            Y_ABORT_UNLESS(
                window->MinimumRSquared >= 0 && window->MinimumRSquared <= 1,
                "cgroup OOM minimum trend R-squared must be in [0, 1]");
        };
        validateTrendWindow(Config.TrendWindows.ShortWindow);
        validateTrendWindow(Config.TrendWindows.LongWindow);
        Y_ABORT_UNLESS(
            !Config.TrendWindows.ShortWindow ||
                !Config.TrendWindows.LongWindow ||
                Config.TrendWindows.ShortWindow->Duration <
                    Config.TrendWindows.LongWindow->Duration,
            "cgroup OOM short trend window must be shorter than the long trend window");
    }

    TSubSystemDependencies TCGroupOomSubSystem::GetDependencies() const {
        // Prefer both providers so the monitor can fall back when the first
        // registered hierarchy is present but has no memory controller.
        return (DependsOn<TCGroupV2StatsSubSystem>() && DependsOn<TCGroupV1StatsSubSystem>()) ||
            DependsOn<TCGroupV2StatsSubSystem>() ||
            DependsOn<TCGroupV1StatsSubSystem>();
    }

    void TCGroupOomSubSystem::OnDependenciesResolved(
            const TResolvedSubSystemDependencies& dependencies) {
        Providers.clear();
        Providers.reserve(dependencies.size());
        for (const auto& dependency : dependencies) {
            Providers.push_back(static_cast<ICGroupMemoryStatsProvider*>(dependency.Instance));
        }
    }

    void TCGroupOomSubSystem::Subscribe(const TActorId& actorId) {
        Y_ABORT_UNLESS(actorId, "cannot subscribe an empty actor id to cgroup OOM alerts");

        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(MonitorActorId, new NDetail::TEvSubscribeActor(actorId));
    }

    void TCGroupOomSubSystem::Unsubscribe(const TActorId& actorId) {
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(MonitorActorId, new NDetail::TEvUnsubscribeActor(actorId));
    }

    void TCGroupOomSubSystem::ReadTrend(
            const TActorId& recipient,
            ECGroupOomTrendWindow window,
            ui64 cookie) const {
        Y_ABORT_UNLESS(recipient, "cannot send a cgroup OOM trend to an empty actor id");
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");
        ValidateTrendWindow(window);

        ActorSystem->Send(
            MonitorActorId,
            new NDetail::TEvReadTrend(recipient, window, cookie));
    }

    void TCGroupOomSubSystem::SubscribeToTrend(
            const TActorId& actorId,
            ECGroupOomTrendWindow window,
            TDuration timeToOomThreshold) {
        Y_ABORT_UNLESS(
            actorId,
            "cannot subscribe an empty actor id to cgroup OOM trends");
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");
        ValidateTrendSubscription(window, timeToOomThreshold);

        ActorSystem->Send(
            MonitorActorId,
            new NDetail::TEvSubscribeTrendActor(
                actorId,
                window,
                timeToOomThreshold));
    }

    void TCGroupOomSubSystem::UnsubscribeFromTrend(const TActorId& actorId) {
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(
            MonitorActorId,
            new NDetail::TEvUnsubscribeTrendActor(actorId));
    }

    void TCGroupOomSubSystem::ValidateTrendWindow(ECGroupOomTrendWindow window) const {
        Y_ABORT_UNLESS(
            Config.TrendWindows.Find(window),
            "cgroup OOM trend window is not configured");
    }

    void TCGroupOomSubSystem::ValidateTrendSubscription(
            ECGroupOomTrendWindow window,
            TDuration timeToOomThreshold) const {
        ValidateTrendWindow(window);
        Y_ABORT_UNLESS(
            timeToOomThreshold > TDuration::Zero(),
            "cgroup OOM trend time threshold must be positive");
    }

    void TCGroupOomSubSystem::OnAfterStart(TActorSystem& actorSystem) {
        const TActorId monitorActorId = actorSystem.Register(
            new NDetail::TCGroupOomActor(Providers, Config),
            TMailboxType::Simple,
            Config.ExecutorPoolId);

        MonitorActorId = monitorActorId;
        ActorSystem = &actorSystem;
    }

    void TCGroupOomSubSystem::OnBeforeStop(TActorSystem& actorSystem) {
        actorSystem.Send(MonitorActorId, new TEvents::TEvPoison());
    }

    std::unique_ptr<TCGroupOomSubSystem> MakeCGroupOomSubSystem(TCGroupOomConfig config) {
        return std::make_unique<TCGroupOomSubSystem>(std::move(config));
    }

    TCGroupOomSubSystem& GetCGroupOomSubSystem(TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TCGroupOomSubSystem>();
        Y_ABORT_UNLESS(subSystem, "cgroup OOM subsystem is not registered");
        return *subSystem;
    }

    TCGroupOomSubSystem& GetCGroupOomSubSystem() {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        return GetCGroupOomSubSystem(*actorSystem);
    }

} // namespace NActors
