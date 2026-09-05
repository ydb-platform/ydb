#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/set_offsets/set_offsets.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/scope.h>
#include <util/thread/pool.h>

#include <atomic>
#include <functional>
#include <optional>

using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NSetOffsets;
using NKikimr::TEvPQ;
using NActors::IEventHandle;
using NActors::TActorId;
using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

namespace {

using TCoreSettings = NKikimr::NPQ::NSetOffsets::TSetOffsetsSettings;

struct TSimulatedSetup {
    THolder<TThreadPool> Pool;
    THolder<::NPersQueue::TTestServer> Server;

    ~TSimulatedSetup() {
        Server.Reset();
        if (Pool) {
            Pool->Stop();
        }
    }

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }
};

TSimulatedSetup& SimulatedCluster() {
    static auto setup = [] {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.SetUseRealThreads(false);

        auto s = std::make_shared<TSimulatedSetup>();
        s->Server = MakeHolder<::NPersQueue::TTestServer>(settings, /*start=*/false);
        s->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

        auto& runtime = s->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::PQ_SCHEMA, NActors::NLog::PRI_DEBUG);
        s->Server->AnnoyingClient->SetNoConfigMode();

        s->Pool = MakeHolder<TThreadPool>();
        s->Pool->Start(2);
        auto* server = s->Server.Get();
        auto future = NThreading::Async([server] {
            server->AnnoyingClient->FullInit();
            return true;
        }, *s->Pool);
        static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
        return s;
    }();
    return *setup;
}

TStatus CreateSimulatedTopic(TSimulatedSetup& setup, const TString& path, const TString& consumer) {
    auto& runtime = setup.GetRuntime();
    TDriverConfig config;
    config.SetEndpoint(setup.Server->Endpoint);
    config.SetDatabase("/Root");
    config.SetAuthToken("root@builtin");
    config.SetDiscoveryMode(EDiscoveryMode::Async);

    auto future = NThreading::Async([config, path, consumer] {
        TDriver driver(config);
        TTopicClient client(driver);
        auto status = client.CreateTopic(path, TCreateTopicSettings()
            .BeginAddConsumer(consumer)
            .EndAddConsumer()).GetValueSync();
        driver.Stop(true);
        return status;
    }, *setup.Pool);
    return static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
}

const TString& SharedSimulatedTopic() {
    static const TString path = [] {
        const TString p = "/Root/sim_topic_shared";
        auto status = CreateSimulatedTopic(SimulatedCluster(), p, "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        return p;
    }();
    return path;
}

struct TRegisteredActor {
    TActorId Edge;
    TActorId Actor;
};

TRegisteredActor CreateActor(NActors::TTestActorRuntime& runtime, TCoreSettings settings) {
    TRegisteredActor registered;
    registered.Edge = runtime.AllocateEdgeActor();
    registered.Actor = runtime.Register(CreateSetOffsetsActor(registered.Edge, std::move(settings)));
    runtime.EnableScheduleForActor(registered.Actor);
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());
    return registered;
}

void ExpectNoResetResult(NActors::TTestActorRuntime& runtime, const TActorId& edge) {
    const auto prev = runtime.SetDispatchTimeout(TDuration::MilliSeconds(1));
    Y_DEFER {
        runtime.SetDispatchTimeout(prev);
    };
    try {
        auto ev = runtime.GrabEdgeEvent<TEvSetOffsetsResult>(edge, TDuration::MilliSeconds(1));
        UNIT_ASSERT(!ev);
    } catch (const NActors::TEmptyEventQueueException&) {
    }
}

THolder<TEvSetOffsetsResult> WaitResult(
    NActors::TTestActorRuntime& runtime,
    const TRegisteredActor& actor,
    TDuration timeout = TDuration::Seconds(30))
{
    auto ev = runtime.GrabEdgeEvent<TEvSetOffsetsResult>(actor.Edge, timeout);
    UNIT_ASSERT_C(ev, "TEvSetOffsetsResult timed out");
    return THolder<TEvSetOffsetsResult>(ev->Release().Release());
}

bool DispatchUntil(NActors::TTestActorRuntime& runtime, std::function<bool()> cond, TDuration timeout = TDuration::Seconds(10)) {
    if (cond()) {
        return true;
    }
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = cond;
    runtime.DispatchEvents(opts, timeout);
    return cond();
}

void AssertAllPartitionsSuccess(const THolder<TEvSetOffsetsResult>& result) {
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Error);
    UNIT_ASSERT(!result->Partitions.empty());
    for (const auto& partition : result->Partitions) {
        UNIT_ASSERT_VALUES_EQUAL_C(partition.Status, Ydb::StatusIds::SUCCESS, partition.Error);
    }
}
class TPipeBreakGuard {
public:
    TPipeBreakGuard(
        NActors::TTestActorRuntime& runtime,
        absl::flat_hash_set<ui32> innerEventTypes,
        size_t maxBreaks = Max<size_t>(),
        std::optional<ui64> onlyTabletId = std::nullopt)
        : Broken_(std::make_shared<std::atomic<size_t>>(0))
    {
        auto broken = Broken_;
        auto types = std::make_shared<absl::flat_hash_set<ui32>>(std::move(innerEventTypes));
        auto* rt = &runtime;

        Observer_ = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>(
            [rt, broken, types, maxBreaks, onlyTabletId](NKikimr::TEvPipeCache::TEvForward::TPtr& ev) {
                if (!ev || !ev->Get()->Ev) {
                    return;
                }
                if (!types->contains(ev->Get()->Ev->Type())) {
                    return;
                }
                const ui64 tabletId = ev->Get()->TabletId;
                if (onlyTabletId && tabletId != *onlyTabletId) {
                    return;
                }
                if (broken->load() >= maxBreaks) {
                    return;
                }

                broken->fetch_add(1);
                const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
                rt->Send(new IEventHandle(
                    ev->Sender,
                    ev->Recipient,
                    new NKikimr::TEvPipeCache::TEvDeliveryProblem(tabletId, true),
                    0,
                    subscribeCookie));
                ev.Reset();
            });
    }

    size_t BrokenCount() const {
        return Broken_->load();
    }

private:
    std::shared_ptr<std::atomic<size_t>> Broken_;
    NActors::TTestActorRuntime::TEventObserverHolder Observer_;
};

} // namespace

Y_UNIT_TEST_SUITE(TSetOffsetsActorSimTests) {

Y_UNIT_TEST(StaleSuccessAcceptedRegardlessOfCookie) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvSetOffsetsRequest::EventType) {
            requested = true;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return requested; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT(requested);

    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
}


Y_UNIT_TEST(SuccessAfterPipeBreakAccepted) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvSetOffsetsRequest::EventType }, /*maxBreaks=*/1);
    auto dropReal = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return pipeBreak.BrokenCount() >= 1; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    // Process DeliveryProblem (WaitRetry=true, PendingPartitions already decremented)
    // without advancing the retry wakeup.
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());

    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
}


Y_UNIT_TEST(LateSuccessDuringWaitRetry) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();

    ui64 requestCookie = 0;
    auto captureCookie = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvSetOffsetsRequest::EventType) {
            return;
        }
        if (!requestCookie) {
            requestCookie = ev->Cookie;
        }
    });
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvSetOffsetsRequest::EventType }, /*maxBreaks=*/1);

    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return pipeBreak.BrokenCount() >= 1 && requestCookie != 0; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    UNIT_ASSERT_VALUES_UNEQUAL(requestCookie, 0u);
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());

    auto dropRetryResponses = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([requestCookie](auto& ev) {
        if (ev->Get()->GetCookie() != requestCookie) {
            ev.Reset();
        }
    });
    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::SUCCESS, {}, requestCookie)));
    AssertAllPartitionsSuccess(WaitResult(runtime, actor, TDuration::Seconds(5)));
}


Y_UNIT_TEST(PoisonDuringDescribe) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    auto dropDescribe = runtime.AddObserver<NDescriber::TEvDescribeTopicsResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/missing",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    runtime.Send(new IEventHandle(actor.Actor, TActorId(), new NActors::TEvents::TEvPoison()));
    ExpectNoResetResult(runtime, actor.Edge);
}


Y_UNIT_TEST(PoisonDuringReset) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvSetOffsetsRequest::EventType) {
            requested = true;
        }
    });
    auto dropResponses = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return requested; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT(requested);

    runtime.Send(new IEventHandle(actor.Actor, TActorId(), new NActors::TEvents::TEvPoison()));
    ExpectNoResetResult(runtime, actor.Edge);
}


Y_UNIT_TEST(UnhandledException) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    auto& flags = runtime.GetAppData().FeatureFlags;
    const bool previous = flags.GetEnableTabletRestartOnUnhandledExceptions();
    flags.SetEnableTabletRestartOnUnhandledExceptions(true);
    Y_DEFER {
        flags.SetEnableTabletRestartOnUnhandledExceptions(previous);
    };
    auto observer = runtime.AddObserver<NDescriber::TEvDescribeTopicsResponse>([](auto& ev) {
        ev->Get()->Topics.clear();
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    auto result = WaitResult(runtime, actor);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, "Unhandled exception");
}


Y_UNIT_TEST(PipeBreakThenSuccess) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvSetOffsetsRequest::EventType }, /*maxBreaks=*/1);
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}


Y_UNIT_TEST(PipeBreakExhausted) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvSetOffsetsRequest::EventType });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    auto result = WaitResult(runtime, actor, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Partitions.size(), 1);
    UNIT_ASSERT_VALUES_UNEQUAL(result->Partitions[0].Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 5u);
}


Y_UNIT_TEST(SchemeErrorFailsWholeRequest) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    ui64 requestCookie = 0;
    auto captureCookie = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvSetOffsetsRequest::EventType) {
            return;
        }
        if (!requestCookie) {
            requestCookie = ev->Cookie;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return requestCookie != 0; }));

    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::SCHEME_ERROR, "Partition 0 not found", requestCookie)));
    auto result = WaitResult(runtime, actor);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, "not found");
}


Y_UNIT_TEST(UnknownPartitionResponseIgnored) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvSetOffsetsRequest::EventType) {
            requested = true;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvSetOffsetsResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return requested; }));

    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(99, Ydb::StatusIds::SUCCESS, {}, 999)));
    ExpectNoResetResult(runtime, actor.Edge);

    runtime.Send(new IEventHandle(actor.Actor, TActorId(),
        new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
}


Y_UNIT_TEST(StaleDeliveryProblemIgnored) {
    auto& setup = SimulatedCluster();
    auto& runtime = setup.GetRuntime();
    ui64 tabletId = 0;
    size_t forwards = 0;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvSetOffsetsRequest::EventType) {
            return;
        }
        ++forwards;
        tabletId = ev->Get()->TabletId;
    });
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = SharedSimulatedTopic(),
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return forwards >= 1 && tabletId != 0; }));
    UNIT_ASSERT_VALUES_EQUAL(forwards, 1u);

    runtime.Send(new IEventHandle(
        actor.Actor,
        TActorId(),
        new NKikimr::TEvPipeCache::TEvDeliveryProblem(tabletId, true),
        0,
        /*stale subscribe cookie*/ 999));
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(forwards, 1u);
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
}

} // TSetOffsetsActorSimTests
