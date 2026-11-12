#include "console_configs_provider.h"
#include "ut_configs_provider_helpers.h"
#include "ut_configs_subscriber_pipe.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <atomic>
#include <array>
#include <memory>
#include <future>

namespace NKikimr::NConsole {
namespace NQueueTestPipe {

TActorId ServiceId(ui32 nodeId) {
    return TActorId(nodeId, "cfg-q-ut");
}

class TClient : public TActorBootstrapped<TClient> {
    const TActorId Owner;
    const ui64 TabletId;
public:
    TClient(TActorId owner, ui64 tabletId)
        : Owner(owner)
        , TabletId(tabletId)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TClient::StateWork);
        ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(
            TabletId, NKikimrProto::OK, SelfId(), {}, true, false, 1));
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TSystem::PoisonPill) {
            PassAway();
        } else {
            // Preserve the real subscriber's Sender, just like tablet-pipe forwarding.
            TActivationContext::Send(ev->Forward(ServiceId(SelfId().NodeId())));
        }
    }
};

IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig&) {
    return new TClient(owner, tabletId);
}

void SendData(const TActorContext& ctx, const TActorId& pipe, IEventBase* event, ui64 cookie) {
    ctx.Send(new IEventHandle(pipe, ctx.SelfID, event, 0, cookie));
}

void CloseClient(const TActorContext& ctx, const TActorId& pipe) {
    ctx.Send(pipe, new TEvents::TEvPoisonPill());
}

} // namespace NQueueTestPipe

namespace {
constexpr ui32 ClientCount = 128;
constexpr ui32 RequestCount = 256;
constexpr ui32 NameserviceNodes = 5310;
constexpr ui64 QueryCookie = ui64(1) << 63;

void ObserveMaximum(std::atomic<ui64>& counter, ui64 value) {
    auto old = counter.load();
    while (old < value && !counter.compare_exchange_weak(old, value)) {
    }
}

struct TDeliveryState {
    std::atomic<ui32> Accepted{0}, Replies{0}, Duplicates{0}, Subscriptions{0}, Fallbacks{0};
    std::atomic<ui64> MaxGeneration{0}, PeakScheduled{0};
    std::atomic<bool> InvalidPayload{false};
    std::promise<void> Done;
};

class TCountingValidator final : public TNameserviceConfigValidator {
public:
    mutable std::atomic<ui64> Checks{0};

    bool CheckConfig(const NKikimrConfig::TAppConfig& oldConfig, const NKikimrConfig::TAppConfig& config,
            TVector<Ydb::Issue::IssueMessage>& issues) const override {
        ++Checks;
        return TNameserviceConfigValidator::CheckConfig(oldConfig, config, issues);
    }
};

class TRouter : public TActorBootstrapped<TRouter> {
    const TActorId Provider;
    const std::shared_ptr<TDeliveryState> State;
public:
    TRouter(TActorId provider, std::shared_ptr<TDeliveryState> state)
        : Provider(provider)
        , State(std::move(state))
    {}

    void Bootstrap(const TActorContext&) { Become(&TRouter::StateWork); }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvConsole::EvConfigSubscriptionRequest) {
            ++State->Subscriptions;
            ObserveMaximum(State->MaxGeneration,
                ev->Get<TEvConsole::TEvConfigSubscriptionRequest>()->Record.GetGeneration());
        } else if (ev->GetTypeRewrite() == TEvConsole::EvGetNodeConfigRequest && !(ev->Cookie & QueryCookie)) {
            ++State->Fallbacks;
        }
        TActivationContext::Send(ev->Forward(Provider));
    }
};

class TRecipients : public TActorBootstrapped<TRecipients> {
    const std::shared_ptr<TDeliveryState> State;
    std::array<bool, ClientCount + 1> Seen{};
public:
    explicit TRecipients(std::shared_ptr<TDeliveryState> state)
        : State(std::move(state))
    {}

    void Bootstrap(const TActorContext&) { Become(&TRecipients::StateWork); }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvConsole::EvConfigSubscriptionNotification) {
            // This is delivery by TConfigsSubscriber AFTER its generation/order checks.
            const auto& record = ev->Get<TEvConsole::TEvConfigSubscriptionNotification>()->Record;
            const auto cookie = ev->Cookie;
            if (!cookie || cookie > ClientCount || record.GetGeneration() != 1 ||
                record.GetConfig().GetNameserviceConfig().NodeSize() != NameserviceNodes) {
                State->InvalidPayload = true;
                return;
            }
            if (Seen[cookie]) {
                ++State->Duplicates;
            } else {
                Seen[cookie] = true;
                ++State->Accepted;
            }
        } else if (ev->GetTypeRewrite() == TEvConsole::EvGetNodeConfigResponse) {
            if (++State->Replies == 1) {
                const auto& record = ev->Get<TEvConsole::TEvGetNodeConfigResponse>()->Record;
                if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS ||
                    record.GetConfig().GetNameserviceConfig().NodeSize() != NameserviceNodes) {
                    State->InvalidPayload = true;
                }
            }
        } else if (ev->GetTypeRewrite() == TEvConsole::EvConfigSubscriptionError) {
            State->InvalidPayload = true;
        }
    }
};

class TProgress : public TActorBootstrapped<TProgress> {
    const std::shared_ptr<TDeliveryState> State;
    const NMonitoring::TDynamicCounters::TCounterPtr Scheduled;
    const NMonitoring::TDynamicCounters::TCounterPtr Inflight;
    const TActorId Recipients;
    bool Started = false;
public:
    TProgress(std::shared_ptr<TDeliveryState> state, NMonitoring::TDynamicCounterPtr counters, TActorId recipients)
        : State(std::move(state))
        , Scheduled(counters->GetCounter("ScheduledConfigUpdates", false))
        , Inflight(counters->GetCounter("InflightConfigUpdates", false))
        , Recipients(recipients)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TProgress::StateWork);
        Tick(ctx);
    }

    void Tick(const TActorContext& ctx) {
        const auto pending = Scheduled->Val();
        ObserveMaximum(State->PeakScheduled, pending);
        if (!Started && pending > 0) {
            Started = true;
            // Finite offered load starts only once actual scheduled work is pending.
            // It is independent of reply speed and never suppresses a real timer.
            for (ui32 i = 0; i < RequestCount; ++i) {
                auto request = MakeHolder<TEvConsole::TEvGetNodeConfigRequest>();
                request->Record.MutableNode()->SetNodeId(10000 + i);
                ctx.Send(new IEventHandle(NQueueTestPipe::ServiceId(SelfId().NodeId()), Recipients,
                    request.Release(), 0, QueryCookie | i));
            }
        }
        if (Started && State->Accepted == ClientCount && State->Replies == RequestCount &&
            pending == 0 && Inflight->Val() == 0) {
            State->Done.set_value();
            PassAway();
        } else {
            ctx.Schedule(TDuration::MilliSeconds(5), new TEvents::TEvWakeup());
        }
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
            Tick(TActivationContext::AsActorContext());
        }
    }
};
} // namespace

Y_UNIT_TEST_SUITE(TConsoleConfigDeliveryTests) {
    Y_UNIT_TEST(QueuedSubscribersDrainDuringEquivalentNodeConfigBurst) {
        auto state = std::make_shared<TDeliveryState>();
        auto validator = MakeIntrusive<TCountingValidator>();
        auto done = state->Done.get_future();
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(registry->AddValidator(validator));
        registry->LockValidators();

        TTestBasicRuntime runtime(1, true);
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("Root", 1).Get());
        runtime.Initialize(app.Unwrap());
        runtime.SetLogPriority(NKikimrServices::CMS_CONFIGS, NLog::PRI_ERROR);
        UNIT_ASSERT(runtime.IsRealThreads());
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto edge = runtime.AllocateEdgeActor();
        const auto provider = runtime.Register(new TConfigsProvider(edge, counters), 0, 0);
        const auto router = runtime.Register(new TRouter(provider, state), 0, 0);
        runtime.RegisterService(NQueueTestPipe::ServiceId(runtime.GetNodeId()), router);
        const auto recipients = runtime.Register(new TRecipients(state), 0, 1);
        auto item = NTests::NameserviceItem(NameserviceNodes);
        for (auto& node : *item->Config.MutableNameserviceConfig()->MutableNode()) {
            const TString host = TStringBuilder() << "static-node-" << node.GetNodeId() << ".cluster.example.test";
            node.SetHost(host);
            node.SetInterconnectHost(host);
            node.SetAddress(TStringBuilder() << "2001:db8::" << node.GetNodeId());
            node.MutableWalleLocation()->SetDataCenter("dc1");
        }
        runtime.Send(new IEventHandle(provider, edge,
            new TConfigsProvider::TEvPrivate::TEvSetConfigs({{1, item}})), 0, true);
        const TString yaml = NTests::MainYaml(87) + "#" + TString(34600, 'x') + "\n";
        runtime.Send(new IEventHandle(provider, edge,
            new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(yaml)), 0, true);
        runtime.Send(new IEventHandle(provider, edge, new TConfigsProvider::TEvPrivate::TEvUpdateConfigs({},
            new IEventHandle(edge, provider, new TEvents::TEvWakeup()))), 0, true);
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge, TDuration::Seconds(30));
        runtime.Register(new TProgress(state, counters, recipients), 0, 0);
        for (ui32 id = 1; id <= ClientCount; ++id) {
            runtime.Register(CreateQueueTestConfigsSubscriber(recipients,
                {NKikimrConsole::TConfigItem::NameserviceConfigItem}, {}, id,
                true, 1, {}, {}, TNodeInfo{"/Root/tenant", "dynamic"}), 0, 1);
        }

        // A generous lifecycle bound, not a speed ratio; correctness also requires
        // accepted owner delivery and gauges0. No virtual-clock pumping is used.
        UNIT_ASSERT_C(done.wait_for(std::chrono::minutes(2)) == std::future_status::ready,
            "accepted=" << state->Accepted.load() << " replies=" << state->Replies.load()
            << " scheduled=" << counters->GetCounter("ScheduledConfigUpdates", false)->Val());
        UNIT_ASSERT(!state->InvalidPayload);
        UNIT_ASSERT_VALUES_EQUAL(state->Duplicates.load(), 0);
        UNIT_ASSERT(state->PeakScheduled > 0);
        UNIT_ASSERT_VALUES_EQUAL(state->Subscriptions.load(), ClientCount);
        UNIT_ASSERT_VALUES_EQUAL(state->MaxGeneration.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state->Fallbacks.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("NodeConfigCacheHits", true)->Val(), RequestCount - 1);
    }
}

} // namespace NKikimr::NConsole
