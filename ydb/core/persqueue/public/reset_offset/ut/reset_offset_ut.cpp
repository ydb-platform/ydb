#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/reset_offset/reset_offset.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/thread/pool.h>

#include <atomic>
#include <functional>
#include <optional>

using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NResetOffset;
using NKikimr::TEvPQ;
using NActors::IEventHandle;
using NActors::TActorId;
using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

namespace {

using TCoreSettings = NKikimr::NPQ::NResetOffset::TResetOffsetSettings;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const char* name) {
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, TTopicSdkTestSetup::MakeServerSettings(), false);
    setup->GetServer().EnableLogs({NKikimrServices::PQ_SCHEMA, NKikimrServices::PERSQUEUE}, NActors::NLog::PRI_DEBUG);
    return setup;
}

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

std::shared_ptr<TSimulatedSetup> CreateSimulatedSetup() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);

    auto setup = std::make_shared<TSimulatedSetup>();
    setup->Server = MakeHolder<::NPersQueue::TTestServer>(settings, /*start=*/false);
    setup->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = setup->GetRuntime();
    runtime.UpdateCurrentTime(TInstant::Now());
    runtime.SetLogPriority(NKikimrServices::PQ_SCHEMA, NActors::NLog::PRI_DEBUG);
    setup->Server->AnnoyingClient->SetNoConfigMode();

    setup->Pool = MakeHolder<TThreadPool>();
    setup->Pool->Start(2);
    auto* server = setup->Server.Get();
    auto future = NThreading::Async([server] {
        server->AnnoyingClient->FullInit();
        return true;
    }, *setup->Pool);
    static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
    return setup;
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

TActorId CreateActor(NActors::TTestActorRuntime& runtime, TCoreSettings settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto actorId = runtime.Register(CreateResetOffsetActor(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(actorId);
    if (runtime.IsRealThreads()) {
        runtime.DispatchEvents();
    } else {
        runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());
    }
    return actorId;
}

THolder<TEvResetOffsetResult> WaitResult(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(30)) {
    auto result = runtime.GrabEdgeEvent<TEvResetOffsetResult>(timeout);
    UNIT_ASSERT_C(result, "TEvResetOffsetResult timed out");
    return result;
}

bool DispatchUntil(NActors::TTestActorRuntime& runtime, std::function<bool()> cond, TDuration timeout = TDuration::Seconds(10)) {
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = cond;
    runtime.DispatchEvents(opts, timeout);
    return cond();
}

void AssertRequestError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode status, const TString& substring) {
    auto result = WaitResult(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, status, result->Error);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, substring);
}

void AssertAllPartitionsSuccess(const THolder<TEvResetOffsetResult>& result) {
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Error);
    UNIT_ASSERT(!result->Partitions.empty());
    for (const auto& partition : result->Partitions) {
        UNIT_ASSERT_VALUES_EQUAL_C(partition.Status, Ydb::StatusIds::SUCCESS, partition.Error);
    }
}

ui64 GetCommittedOffset(TTopicSdkTestSetup& setup, const TString& topic, const TString& consumer, ui32 partitionId = 0) {
    auto describe = setup.DescribeConsumer(topic, consumer);
    UNIT_ASSERT_LT(partitionId, describe.GetPartitions().size());
    const auto& stats = describe.GetPartitions()[partitionId].GetPartitionConsumerStats();
    UNIT_ASSERT(stats);
    return stats->GetCommittedOffset();
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

Y_UNIT_TEST_SUITE(TResetOffsetActorTests) {

Y_UNIT_TEST(TopicNotExists) {
    auto setup = CreateSetup("ResetOffsetTopicNotExists");
    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic_not_exists",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertRequestError(runtime, Ydb::StatusIds::SCHEME_ERROR, "does not exist");
}

Y_UNIT_TEST(TopicWithoutConsumer) {
    auto setup = CreateSetup("ResetOffsetNoConsumer");
    setup->CreateTopic("topic1", "other-consumer");
    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer_not_exists",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertRequestError(runtime, Ydb::StatusIds::SCHEME_ERROR, "does not exist");
}

Y_UNIT_TEST(Unauthorized) {
    auto setup = CreateSetup("ResetOffsetUnauthorized");
    setup->CreateTopic("topic1", "consumer");

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
    setup->GetServer().AnnoyingClient->ModifyACL("/Root", "topic1", acl.SerializeAsString());

    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
        .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
    });
    auto result = WaitResult(runtime);
    UNIT_ASSERT(result->Status == Ydb::StatusIds::SCHEME_ERROR || result->Status == Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(!result->Error.empty());
}

Y_UNIT_TEST(MlpConsumerRejected) {
    auto setup = CreateSetup("ResetOffsetMlp");
    TTopicClient client(setup->MakeDriver());
    auto status = client.CreateTopic(setup->GetFullTopicPath("topic1"), TCreateTopicSettings()
        .BeginAddSharedConsumer("mlp-consumer")
        .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    setup->GetServer().WaitInit(setup->GetTopicPath("topic1"));

    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "mlp-consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertRequestError(runtime, Ydb::StatusIds::BAD_REQUEST, "MLP");
}

Y_UNIT_TEST(EmptyTopicEarliest) {
    auto setup = CreateSetup("ResetOffsetEmptyEarliest");
    setup->CreateTopic("topic1", "consumer");
    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 0);
}

Y_UNIT_TEST(EmptyTopicLatest) {
    auto setup = CreateSetup("ResetOffsetEmptyLatest");
    setup->CreateTopic("topic1", "consumer");
    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::LATEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 0);
}

Y_UNIT_TEST(ManyPartitions) {
    auto setup = CreateSetup("ResetOffsetManyPartitions");
    setup->CreateTopic("topic1", "consumer", 4);
    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::LATEST,
    });
    auto result = WaitResult(runtime);
    AssertAllPartitionsSuccess(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Partitions.size(), 4);
}

Y_UNIT_TEST(RewindActiveAfterWrite) {
    auto setup = CreateSetup("ResetOffsetRewindActive");
    setup->CreateTopic("topic1", "consumer");
    setup->Write("topic1", "m1", 0);
    setup->Write("topic1", "m2", 0);

    auto client = setup->MakeClient();
    UNIT_ASSERT(client.CommitOffset(setup->GetFullTopicPath("topic1"), 0, "consumer", 2).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 2);

    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 0);
}

Y_UNIT_TEST(SkipToEnd) {
    auto setup = CreateSetup("ResetOffsetSkipToEnd");
    setup->CreateTopic("topic1", "consumer");
    setup->Write("topic1", "m1", 0);
    setup->Write("topic1", "m2", 0);

    auto& runtime = setup->GetRuntime();
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::LATEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 2);
}

Y_UNIT_TEST(TimestampBeforeInsideAfter) {
    auto setup = CreateSetup("ResetOffsetTimestamp");
    setup->CreateTopic("topic1", "consumer");
    const auto before = TInstant::Now() - TDuration::Hours(1);
    setup->Write("topic1", "m1", 0);
    Sleep(TDuration::MilliSeconds(50));
    const auto middle = TInstant::Now();
    Sleep(TDuration::MilliSeconds(50));
    setup->Write("topic1", "m2", 0);
    const auto after = TInstant::Now() + TDuration::Hours(1);

    auto& runtime = setup->GetRuntime();
    auto resetAt = [&](TInstant ts) {
        CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = TString{setup->GetFullTopicPath("topic1")},
            .Consumer = "consumer",
            .Position = NKikimrPQ::TEvResetOffsetRequest::FROM_WRITTEN_AT,
            .TimestampMs = ts.MilliSeconds(),
        });
        AssertAllPartitionsSuccess(WaitResult(runtime));
        return GetCommittedOffset(*setup, "topic1", "consumer");
    };

    UNIT_ASSERT_VALUES_EQUAL(resetAt(before), 0);
    const auto middleOffset = resetAt(middle);
    UNIT_ASSERT(middleOffset == 1 || middleOffset == 0);
    UNIT_ASSERT_VALUES_EQUAL(resetAt(after), 2);
}

Y_UNIT_TEST(StaleCookieIgnored) {
    auto setup = CreateSetup("ResetOffsetStaleCookie");
    setup->CreateTopic("topic1", "consumer", 1);
    auto& runtime = setup->GetRuntime();
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup->GetFullTopicPath("topic1")},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    runtime.Send(new IEventHandle(actorId, TActorId(), new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::GENERIC_ERROR, "stale", 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime));
}

Y_UNIT_TEST(StaleSuccessAcceptedRegardlessOfCookie) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvResetOffsetRequest::EventType) {
            requested = true;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return requested; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT(requested);

    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime));
}

Y_UNIT_TEST(SuccessAfterPipeBreakAccepted) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvResetOffsetRequest::EventType }, /*maxBreaks=*/1);
    auto dropReal = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return pipeBreak.BrokenCount() >= 1; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    // Process DeliveryProblem (WaitRetry=true, PendingPartitions already decremented)
    // without advancing the retry wakeup.
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());

    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime));
}

Y_UNIT_TEST(LateSuccessDuringWaitRetry) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();

    ui64 requestCookie = 0;
    auto captureCookie = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvResetOffsetRequest::EventType) {
            return;
        }
        if (!requestCookie) {
            requestCookie = ev->Cookie;
        }
    });
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvResetOffsetRequest::EventType }, /*maxBreaks=*/1);

    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return pipeBreak.BrokenCount() >= 1 && requestCookie != 0; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    UNIT_ASSERT_VALUES_UNEQUAL(requestCookie, 0u);
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Zero());

    auto dropRetryResponses = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([requestCookie](auto& ev) {
        if (ev->Get()->GetCookie() != requestCookie) {
            ev.Reset();
        }
    });
    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::SUCCESS, {}, requestCookie)));
    AssertAllPartitionsSuccess(WaitResult(runtime, TDuration::Seconds(5)));
}

Y_UNIT_TEST(PoisonDuringDescribe) {
    auto setup = CreateSimulatedSetup();
    auto& runtime = setup->GetRuntime();
    auto dropDescribe = runtime.AddObserver<NDescriber::TEvDescribeTopicsResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/missing",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    runtime.Send(new IEventHandle(actorId, TActorId(), new NActors::TEvents::TEvPoison()));
    auto result = runtime.GrabEdgeEvent<TEvResetOffsetResult>(TDuration::Seconds(2));
    UNIT_ASSERT(!result);
}

Y_UNIT_TEST(PoisonDuringReset) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvResetOffsetRequest::EventType) {
            requested = true;
        }
    });
    auto dropResponses = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&] { return requested; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
    UNIT_ASSERT(requested);

    runtime.Send(new IEventHandle(actorId, TActorId(), new NActors::TEvents::TEvPoison()));
    auto result = runtime.GrabEdgeEvent<TEvResetOffsetResult>(TDuration::Seconds(2));
    UNIT_ASSERT(!result);
}

Y_UNIT_TEST(UnhandledException) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);
    auto observer = runtime.AddObserver<NDescriber::TEvDescribeTopicsResponse>([](auto& ev) {
        ev->Get()->Topics.clear();
    });
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    auto result = WaitResult(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, "Unhandled exception");
}

Y_UNIT_TEST(PipeBreakThenSuccess) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvResetOffsetRequest::EventType }, /*maxBreaks=*/1);
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(PipeBreakExhausted) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvResetOffsetRequest::EventType });
    CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    auto result = WaitResult(runtime, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Partitions.size(), 1);
    UNIT_ASSERT_VALUES_UNEQUAL(result->Partitions[0].Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 5u);
}

Y_UNIT_TEST(TabletDirectEarliestLatest) {
    auto setup = CreateSetup("ResetOffsetTabletDirect");
    setup->CreateTopic("topic1", "consumer");
    setup->Write("topic1", "m1", 0);

    auto edge = setup->GetRuntime().AllocateEdgeActor();
    NDescriber::TDescribeSettings describeSettings;
    auto describer = setup->GetRuntime().Register(NDescriber::CreateDescriberActor(
        edge, "/Root", { TString{setup->GetFullTopicPath("topic1")} }, describeSettings));
    Y_UNUSED(describer);
    auto described = setup->GetRuntime().GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(described);
    const auto& topic = described->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    const ui64 tabletId = topic.Info->Description.GetPartitions(0).GetTabletId();
    const TString path = TString{setup->GetFullTopicPath("topic1")};

    NKikimr::ForwardToTablet(setup->GetRuntime(), tabletId, edge,
        new TEvPQ::TEvResetOffsetRequest(path, "consumer", 0, NKikimrPQ::TEvResetOffsetRequest::LATEST));
    auto latest = setup->GetRuntime().GrabEdgeEvent<TEvPQ::TEvResetOffsetResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(latest);
    UNIT_ASSERT_VALUES_EQUAL(latest->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 1);

    NKikimr::ForwardToTablet(setup->GetRuntime(), tabletId, edge,
        new TEvPQ::TEvResetOffsetRequest(path, "consumer", 0, NKikimrPQ::TEvResetOffsetRequest::EARLIEST));
    auto earliest = setup->GetRuntime().GrabEdgeEvent<TEvPQ::TEvResetOffsetResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(earliest);
    UNIT_ASSERT_VALUES_EQUAL(earliest->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 0);
}

Y_UNIT_TEST(TabletDirectUnspecifiedPosition) {
    auto setup = CreateSetup("ResetOffsetTabletUnspecified");
    setup->CreateTopic("topic1", "consumer");

    auto edge = setup->GetRuntime().AllocateEdgeActor();
    NDescriber::TDescribeSettings describeSettings;
    setup->GetRuntime().Register(NDescriber::CreateDescriberActor(
        edge, "/Root", { TString{setup->GetFullTopicPath("topic1")} }, describeSettings));
    auto described = setup->GetRuntime().GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(described);
    const auto& topic = described->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT(topic.Info);
    const ui64 tabletId = topic.Info->Description.GetPartitions(0).GetTabletId();
    const TString path = TString{setup->GetFullTopicPath("topic1")};

    NKikimr::ForwardToTablet(setup->GetRuntime(), tabletId, edge,
        new TEvPQ::TEvResetOffsetRequest(path, "consumer", 0, NKikimrPQ::TEvResetOffsetRequest::POSITION_UNSPECIFIED));
    auto response = setup->GetRuntime().GrabEdgeEvent<TEvPQ::TEvResetOffsetResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), Ydb::StatusIds::BAD_REQUEST);
}

Y_UNIT_TEST(TabletDirectUnknownPartition) {
    auto setup = CreateSetup("ResetOffsetTabletUnknownPart");
    setup->CreateTopic("topic1", "consumer");

    auto edge = setup->GetRuntime().AllocateEdgeActor();
    NDescriber::TDescribeSettings describeSettings;
    setup->GetRuntime().Register(NDescriber::CreateDescriberActor(
        edge, "/Root", { TString{setup->GetFullTopicPath("topic1")} }, describeSettings));
    auto described = setup->GetRuntime().GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(described);
    const auto& topic = described->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT(topic.Info);
    const ui64 tabletId = topic.Info->Description.GetPartitions(0).GetTabletId();
    const TString path = TString{setup->GetFullTopicPath("topic1")};

    NKikimr::ForwardToTablet(setup->GetRuntime(), tabletId, edge,
        new TEvPQ::TEvResetOffsetRequest(path, "consumer", 999, NKikimrPQ::TEvResetOffsetRequest::EARLIEST, 0, 42));
    auto response = setup->GetRuntime().GrabEdgeEvent<TEvPQ::TEvResetOffsetResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(response->GetErrorMessage(), "not found");
}

Y_UNIT_TEST(TabletDirectResetDoesNotStealCommit) {
    auto setup = CreateSetup("ResetOffsetTabletNoSteal");
    setup->CreateTopic("topic1", "consumer");
    setup->Write("topic1", "m1", 0);

    auto client = setup->MakeClient();
    const auto path = setup->GetFullTopicPath("topic1");
    UNIT_ASSERT(client.CommitOffset(path, 0, "consumer", 1).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 1);

    auto edge = setup->GetRuntime().AllocateEdgeActor();
    NDescriber::TDescribeSettings describeSettings;
    setup->GetRuntime().Register(NDescriber::CreateDescriberActor(
        edge, "/Root", { TString{path} }, describeSettings));
    auto described = setup->GetRuntime().GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(described);
    const auto& topic = described->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT(topic.Info);
    const ui64 tabletId = topic.Info->Description.GetPartitions(0).GetTabletId();

    NKikimr::ForwardToTablet(setup->GetRuntime(), tabletId, edge,
        new TEvPQ::TEvResetOffsetRequest(TString{path}, "consumer", 0, NKikimrPQ::TEvResetOffsetRequest::EARLIEST, 0, 1));
    auto reset = setup->GetRuntime().GrabEdgeEvent<TEvPQ::TEvResetOffsetResponse>(TDuration::Seconds(30));
    UNIT_ASSERT(reset);
    UNIT_ASSERT_VALUES_EQUAL(reset->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(reset->GetCookie(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 0);

    UNIT_ASSERT(client.CommitOffset(path, 0, "consumer", 1).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(*setup, "topic1", "consumer"), 1);
}

Y_UNIT_TEST(SchemeErrorFailsWholeRequest) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    ui64 requestCookie = 0;
    auto captureCookie = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvResetOffsetRequest::EventType) {
            return;
        }
        if (!requestCookie) {
            requestCookie = ev->Cookie;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([](auto& ev) {
        ev.Reset();
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return requestCookie != 0; }));

    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::SCHEME_ERROR, "Partition 0 not found", requestCookie)));
    auto result = WaitResult(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, "not found");
}

Y_UNIT_TEST(UnknownPartitionResponseIgnored) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    bool requested = false;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev && ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPQ::TEvResetOffsetRequest::EventType) {
            requested = true;
        }
    });
    auto dropReal = runtime.AddObserver<TEvPQ::TEvResetOffsetResponse>([](auto& ev) {
        if (ev->Get()->GetCookie() != 999) {
            ev.Reset();
        }
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return requested; }));

    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(99, Ydb::StatusIds::SUCCESS, {}, 999)));
    auto early = runtime.GrabEdgeEvent<TEvResetOffsetResult>(TDuration::MilliSeconds(200));
    UNIT_ASSERT(!early);

    runtime.Send(new IEventHandle(actorId, TActorId(),
        new TEvPQ::TEvResetOffsetResponse(0, Ydb::StatusIds::SUCCESS, {}, 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime));
}

Y_UNIT_TEST(StaleDeliveryProblemIgnored) {
    auto setup = CreateSimulatedSetup();
    {
        auto status = CreateSimulatedTopic(*setup, "/Root/topic1", "consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    auto& runtime = setup->GetRuntime();
    ui64 tabletId = 0;
    size_t forwards = 0;
    auto watch = runtime.AddObserver<NKikimr::TEvPipeCache::TEvForward>([&](auto& ev) {
        if (!ev || !ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != TEvPQ::TEvResetOffsetRequest::EventType) {
            return;
        }
        ++forwards;
        tabletId = ev->Get()->TabletId;
    });
    auto actorId = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST,
    });
    UNIT_ASSERT(DispatchUntil(runtime, [&] { return forwards >= 1 && tabletId != 0; }));
    UNIT_ASSERT_VALUES_EQUAL(forwards, 1u);

    runtime.Send(new IEventHandle(
        actorId,
        TActorId(),
        new NKikimr::TEvPipeCache::TEvDeliveryProblem(tabletId, true),
        0,
        /*stale subscribe cookie*/ 999));
    runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(forwards, 1u);
    AssertAllPartitionsSuccess(WaitResult(runtime));
}

} // TResetOffsetActorTests
