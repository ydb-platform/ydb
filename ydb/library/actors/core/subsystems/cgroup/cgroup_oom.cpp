#include "cgroup_oom.h"

#include "cgroup_events.h"
#include "cgroup_v1.h"
#include "cgroup_v2.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NActors {

    namespace {

        ui32 ReasonMask(ECGroupOomReason reason) {
            return static_cast<ui32>(reason);
        }

        ui64 CounterDelta(ui64 current, ui64 previous) {
            return current >= previous ? current - previous : 0;
        }

        ui32 ExtractSubscriberActivityIndex(const TActorId& actorId) {
            if (!TlsActivationContext) {
                return Max<ui32>();
            }

            const TActorContext& context = TActivationContext::AsActorContext();
            if (actorId.NodeId() != context.SelfID.NodeId()) {
                return Max<ui32>();
            }
            if (IActor* actor = context.Mailbox.FindActor(actorId.LocalId())) {
                return actor->GetActivityType().GetIndex();
            }
            if (IActor* actor = context.Mailbox.FindAlias(actorId.LocalId())) {
                return actor->GetActivityType().GetIndex();
            }
            return Max<ui32>();
        }

        TStringBuf FormatSubscriberActivityName(ui32 activityIndex) {
            return activityIndex == Max<ui32>() ? TStringBuf("manual") : GetActivityTypeName(activityIndex);
        }

    } // namespace

    namespace NDetail {

        enum : ui32 {
            EvPoll = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvSubscribeCallback,
            EvUnsubscribeCallback,
            EvSubscribeActor,
            EvUnsubscribeActor,
            EvCheckActorSubscribersLiveness,
        };

        struct TEvPoll : TEventLocal<TEvPoll, EvPoll> {
        };

        struct TEvSubscribeCallback : TEventLocal<TEvSubscribeCallback, EvSubscribeCallback> {
            TCGroupOomSubSystem::TSubscriptionId SubscriptionId;
            TCGroupOomSubSystem::TCallback Callback;

            TEvSubscribeCallback(
                    TCGroupOomSubSystem::TSubscriptionId subscriptionId,
                    TCGroupOomSubSystem::TCallback callback)
                : SubscriptionId(subscriptionId)
                , Callback(std::move(callback))
            {
            }
        };

        struct TEvUnsubscribeCallback : TEventLocal<TEvUnsubscribeCallback, EvUnsubscribeCallback> {
            TCGroupOomSubSystem::TSubscriptionId SubscriptionId;

            explicit TEvUnsubscribeCallback(TCGroupOomSubSystem::TSubscriptionId subscriptionId)
                : SubscriptionId(subscriptionId)
            {
            }
        };

        struct TEvSubscribeActor : TEventLocal<TEvSubscribeActor, EvSubscribeActor> {
            TActorId ActorId;
            ui32 ActivityIndex;

            TEvSubscribeActor(TActorId actorId, ui32 activityIndex)
                : ActorId(actorId)
                , ActivityIndex(activityIndex)
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

        struct TEvCheckActorSubscribersLiveness
            : TEventLocal<TEvCheckActorSubscribersLiveness, EvCheckActorSubscribersLiveness>
        {
        };

        class TCGroupOomSubscriberLivenessChecker
            : public TActorBootstrapped<TCGroupOomSubscriberLivenessChecker>
        {
        public:
            TCGroupOomSubscriberLivenessChecker(
                    const TActorId& subscriptionOwner,
                    THashMap<TActorId, ui32> subscribers)
                : SubscriptionOwner(subscriptionOwner)
                , PendingSubscribers(std::move(subscribers))
            {
            }

            void Bootstrap() {
                if (PendingSubscribers.empty()) {
                    return PassAway();
                }

                Become(&TThis::StateWork);
                // Every liveness probe is guaranteed to eventually produce exactly one response:
                // ActorAlive or ActorDead for a local target, and ActorLivenessUnsure for a remote one.
                // The checker can therefore wait for all responses without a timeout.
                for (const auto& [subscriber, activityIndex] : PendingSubscribers) {
                    Y_UNUSED(activityIndex);
                    TActivationContext::Send(new IEventHandle(
                        TEvents::TSystem::CheckActorLiveness,
                        TEvents::TEvCheckActorLiveness::RequestFlags,
                        subscriber,
                        SelfId(),
                        nullptr,
                        0));
                }
            }

        private:
            STRICT_STFUNC(StateWork,
                hFunc(TEvents::TEvActorAlive, Handle)
                hFunc(TEvents::TEvActorDead, Handle)
                hFunc(TEvents::TEvActorLivenessUnsure, Handle)
            )

            void Handle(TEvents::TEvActorAlive::TPtr& ev) {
                Complete(ev->Sender);
            }

            void Handle(TEvents::TEvActorDead::TPtr& ev) {
                if (const auto it = PendingSubscribers.find(ev->Sender);
                        it != PendingSubscribers.end()) {
                    ++LeakedSubscribersByActivity[it->second];
                    PendingSubscribers.erase(it);
                    Send(SubscriptionOwner, new TEvUnsubscribeActor(ev->Sender));
                    PassAwayIfDone();
                }
            }

            void Handle(TEvents::TEvActorLivenessUnsure::TPtr& ev) {
                Complete(ev->Sender);
            }

            void Complete(const TActorId& subscriber) {
                if (PendingSubscribers.erase(subscriber)) {
                    PassAwayIfDone();
                }
            }

            void PassAwayIfDone() {
                if (PendingSubscribers.empty()) {
                    LogLeakedSubscribers();
                    PassAway();
                }
            }

            void LogLeakedSubscribers() const {
                if (LeakedSubscribersByActivity.empty()) {
                    return;
                }

                TStringBuilder details;
                bool first = true;
                for (const auto& [activityIndex, count] : LeakedSubscribersByActivity) {
                    if (!first) {
                        details << ", ";
                    }
                    first = false;
                    details << "{activity# " << FormatSubscriberActivityName(activityIndex)
                        << " actors# " << count << '}';
                }
                NLog::TSettings* loggerSettings = TlsActivationContext->LoggerSettings();
                if (loggerSettings &&
                        loggerSettings->Satisfies(NLog::PRI_WARN, NActorsServices::GLOBAL)) {
                    Send(loggerSettings->LoggerActorId, new NLog::TEvLog(
                        NLog::PRI_WARN,
                        NActorsServices::GLOBAL,
                        TStringBuilder()
                            << "CGroup OOM subscriber liveness check found leaked subscriptions: "
                            << details));
                }
            }

        private:
            const TActorId SubscriptionOwner;
            THashMap<TActorId, ui32> PendingSubscribers;
            TMap<ui32, ui64> LeakedSubscribersByActivity;
        };

        class TCGroupOomActor : public TActorBootstrapped<TCGroupOomActor> {
        public:
            TCGroupOomActor(
                    TVector<const ICGroupMemoryStatsProvider*> providers,
                    TCGroupOomConfig config)
                : Providers(std::move(providers))
                , Config(std::move(config))
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
                Send(SelfId(), new TEvPoll());
                if (Config.SubscriberLivenessCheckInterval != TDuration::Zero()) {
                    Schedule(Config.SubscriberLivenessCheckInterval,
                        new TEvCheckActorSubscribersLiveness());
                }
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvPoll, Handle)
                hFunc(TEvCGroupMemoryStats, Handle)
                hFunc(TEvSubscribeCallback, Handle)
                hFunc(TEvUnsubscribeCallback, Handle)
                hFunc(TEvSubscribeActor, Handle)
                hFunc(TEvUnsubscribeActor, Handle)
                hFunc(TEvCheckActorSubscribersLiveness, Handle)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )

        private:
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
                    Evaluate(std::move(ev->Get()->Stats));
                    Schedule(Config.PollPeriod, new TEvPoll());
                } else if (providerIndex + 1 < Providers.size()) {
                    RequestProvider(providerIndex + 1);
                } else {
                    Schedule(Config.PollPeriod, new TEvPoll());
                }
            }

            void Handle(TEvSubscribeCallback::TPtr& ev) {
                Callbacks.emplace(ev->Get()->SubscriptionId, std::move(ev->Get()->Callback));
            }

            void Handle(TEvUnsubscribeCallback::TPtr& ev) {
                Callbacks.erase(ev->Get()->SubscriptionId);
            }

            void Handle(TEvSubscribeActor::TPtr& ev) {
                ActorSubscribers[ev->Get()->ActorId] = ev->Get()->ActivityIndex;
            }

            void Handle(TEvUnsubscribeActor::TPtr& ev) {
                ActorSubscribers.erase(ev->Get()->ActorId);
            }

            void Handle(TEvCheckActorSubscribersLiveness::TPtr&) {
                const TDuration interval = Config.SubscriberLivenessCheckInterval;
                if (interval == TDuration::Zero()) {
                    return;
                }

                if (!ActorSubscribers.empty()) {
                    TActivationContext::Register(
                        new TCGroupOomSubscriberLivenessChecker(SelfId(), ActorSubscribers),
                        SelfId());
                }
                Schedule(interval, new TEvCheckActorSubscribersLiveness());
            }

            void Evaluate(TCGroupMemoryStatsPtr stats) {
                if (!PreviousVersion || *PreviousVersion != stats->Version ||
                        PreviousCGroupPath != stats->CGroupPath) {
                    PreviousVersion = stats->Version;
                    PreviousCGroupPath = stats->CGroupPath;
                    HasPreviousMemory = false;
                    ActiveLevelReasons = 0;
                }

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
            }

            void Notify(const TCGroupOomAlert& alert) {
                const TActorContext& actorContext = TActivationContext::AsActorContext();
                for (const auto& [actorId, activityIndex] : ActorSubscribers) {
                    Y_UNUSED(activityIndex);
                    actorContext.Send(actorId, new TEvCGroupOomAlert(alert));
                }

                for (const auto& item : Callbacks) {
                    try {
                        item.second(alert);
                    } catch (...) {
                        // One callback must not prevent other subscribers from
                        // releasing memory.
                    }
                }
            }

        private:
            const TVector<const ICGroupMemoryStatsProvider*> Providers;
            const TCGroupOomConfig Config;

            THashMap<TCGroupOomSubSystem::TSubscriptionId, TCGroupOomSubSystem::TCallback> Callbacks;
            THashMap<TActorId, ui32> ActorSubscribers;

            std::optional<ECGroupVersion> PreviousVersion;
            TString PreviousCGroupPath;
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

    TCGroupOomSubSystem::TSubscriptionId TCGroupOomSubSystem::Subscribe(TCallback callback) {
        Y_ABORT_UNLESS(callback, "cannot subscribe an empty cgroup OOM callback");

        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        const TSubscriptionId subscriptionId =
            NextSubscriptionId.fetch_add(1, std::memory_order_relaxed);
        ActorSystem->Send(MonitorActorId,
            new NDetail::TEvSubscribeCallback(subscriptionId, std::move(callback)));
        return subscriptionId;
    }

    void TCGroupOomSubSystem::Unsubscribe(TSubscriptionId subscriptionId) {
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(MonitorActorId,
            new NDetail::TEvUnsubscribeCallback(subscriptionId));
    }

    void TCGroupOomSubSystem::Subscribe(const TActorId& actorId) {
        Y_ABORT_UNLESS(actorId, "cannot subscribe an empty actor id to cgroup OOM alerts");

        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(MonitorActorId,
            new NDetail::TEvSubscribeActor(actorId, ExtractSubscriberActivityIndex(actorId)));
    }

    void TCGroupOomSubSystem::Unsubscribe(const TActorId& actorId) {
        Y_ABORT_UNLESS(ActorSystem, "cgroup OOM subsystem is not running");

        ActorSystem->Send(MonitorActorId, new NDetail::TEvUnsubscribeActor(actorId));
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
