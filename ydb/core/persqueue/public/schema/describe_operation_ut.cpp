#include "schema_ut_helpers.h"

#include "describe_operation.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <memory>
#include <optional>

namespace NKikimr::NPQ::NSchema {

using namespace NTests;

namespace {

struct TSimulatedServer {
    THolder<TThreadPool> Pool;
    THolder<::NPersQueue::TTestServer> Server;

    ~TSimulatedServer() {
        Server.Reset();
        if (Pool) {
            Pool->Stop();
        }
    }

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }
};

std::unique_ptr<TSimulatedServer> CreateSimulatedServer() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);

    auto out = std::make_unique<TSimulatedServer>();
    out->Server = MakeHolder<::NPersQueue::TTestServer>(settings, /*start=*/false);
    out->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = out->GetRuntime();
    runtime.UpdateCurrentTime(TInstant::Now());
    out->Server->EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);

    out->Server->AnnoyingClient->SetNoConfigMode();
    out->Pool = MakeHolder<TThreadPool>();
    out->Pool->Start(2);
    auto* server = out->Server.Get();
    auto future = NThreading::Async([server] {
        server->AnnoyingClient->FullInit();
        return true;
    }, *out->Pool);
    static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
    return out;
}

class TEnableScheduleForRootGuard {
public:
    explicit TEnableScheduleForRootGuard(NActors::TTestActorRuntime& runtime)
        : Runtime(runtime)
        , RootActorId(std::make_shared<TActorId>())
    {
        PrevObserver = Runtime.SetRegistrationObserverFunc(
            [rootActorId = RootActorId](
                TTestActorRuntimeBase& rt,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                if (actorId == *rootActorId || parentId == *rootActorId) {
                    rt.EnableScheduleForActor(actorId);
                }
            });
    }

    ~TEnableScheduleForRootGuard() {
        Runtime.SetRegistrationObserverFunc(std::move(PrevObserver));
    }

    TEnableScheduleForRootGuard(const TEnableScheduleForRootGuard&) = delete;
    TEnableScheduleForRootGuard& operator=(const TEnableScheduleForRootGuard&) = delete;

    void SetRoot(const TActorId& actorId) {
        *RootActorId = actorId;
        Runtime.EnableScheduleForActor(actorId, true);
    }

    const TActorId& GetRoot() const {
        return *RootActorId;
    }

private:
    NActors::TTestActorRuntime& Runtime;
    std::shared_ptr<TActorId> RootActorId;
    TTestActorRuntimeBase::TRegistrationObserver PrevObserver;
};

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, ui32 partitions) {
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path, partitions)), Ydb::StatusIds::SUCCESS);
}

struct TTestDescribeStrategyOptions {
    std::optional<TDescribeSchemaError> ValidateError;
    std::optional<ui32> OnlyPartitionId;
    bool WithReadSessions = false;
    bool WithStatus = false;
    TString ConsumerName;
    // Unset: fail on every ValidateSchema call while ValidateError is set.
    std::optional<ui32> FailValidateTimes;
    ui32* ValidateCalls = nullptr;
};

class TTestDescribeStrategy: public IDescribeStrategy {
public:
    TTestDescribeStrategy() = default;

    explicit TTestDescribeStrategy(TTestDescribeStrategyOptions options)
        : Options(std::move(options))
    {
    }

    TString GetName() const override {
        return "TestDescribe";
    }

    TDescribeSchemaResult ValidateSchema(const NDescriber::TTopicInfo&) override {
        if (Options.ValidateCalls) {
            ++*Options.ValidateCalls;
        }
        if (Options.ValidateError) {
            const bool shouldFail = !Options.FailValidateTimes
                || (Options.ValidateCalls && *Options.ValidateCalls <= *Options.FailValidateTimes);
            if (shouldFail) {
                return {.Error = Options.ValidateError};
            }
        }
        return {.ConsumerName = Options.ConsumerName};
    }

    bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) const override {
        return !Options.OnlyPartitionId || *Options.OnlyPartitionId == partition.GetPartitionId();
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const override {
        if (!Options.WithReadSessions) {
            return nullptr;
        }
        return std::make_unique<TEvPersQueue::TEvGetReadSessionsInfo>(Options.ConsumerName);
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const override {
        if (!Options.WithStatus) {
            return nullptr;
        }
        if (Options.ConsumerName) {
            return std::make_unique<TEvPersQueue::TEvStatus>(Options.ConsumerName);
        }
        return std::make_unique<TEvPersQueue::TEvStatus>();
    }

private:
    TTestDescribeStrategyOptions Options;
};

THolder<TEvDescribeOperationResponse> RunDescribeOperation(
    NActors::TTestActorRuntime& runtime,
    TDescribeOperationSettings settings,
    std::unique_ptr<IDescribeStrategy> strategy,
    TDuration waitTimeout = TDuration::Seconds(30))
{
    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeOperationActor(
        edge, std::move(settings), std::move(strategy))));
    runtime.DispatchEvents();
    auto handle = runtime.GrabEdgeEvent<TEvDescribeOperationResponse>(edge, waitTimeout);
    UNIT_ASSERT(handle);
    return THolder(handle->Release());
}

TDescribeOperationSettings MakeSettings(
    const TString& path,
    bool includeLocation = false,
    bool includeStats = false)
{
    return {
        .Path = path,
        .Database = "/Root",
        .AccessRights = NACLib::EAccessRights::DescribeSchema,
        .IncludeStats = includeStats,
        .IncludeLocation = includeLocation,
    };
}

auto BreakFirstLocationForward(NActors::TTestActorRuntime& runtime, size_t& broken) {
    auto* rt = &runtime;
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&broken, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPersQueue::TEvGetPartitionsLocation::EventType) {
                return;
            }
            if (broken >= 1) {
                return;
            }
            ++broken;
            const ui64 tabletId = ev->Get()->TabletId;
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
            rt->Send(new IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true /*notDelivered*/),
                0,
                subscribeCookie));
            ev.Reset();
        });
}

auto DropLocationForwards(NActors::TTestActorRuntime& runtime) {
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
        [](TEvPipeCache::TEvForward::TPtr& ev) {
            if (ev && ev->Get()->Ev &&
                ev->Get()->Ev->Type() == TEvPersQueue::TEvGetPartitionsLocation::EventType)
            {
                ev.Reset();
            }
        });
}

auto InjectEmptyStatusOnce(NActors::TTestActorRuntime& runtime, size_t& injected) {
    auto* rt = &runtime;
    return runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&injected, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPersQueue::TEvStatus::EventType) {
                return;
            }
            if (injected >= 1) {
                return;
            }
            ++injected;
            auto* response = new TEvPersQueue::TEvStatusResponse();
            rt->Send(new IEventHandle(ev->Sender, ev->Recipient, response, 0, ev->Cookie));
            ev.Reset();
        });
}

} // namespace

Y_UNIT_TEST_SUITE(DescribeOperationActor) {

Y_UNIT_TEST(SuccessWithoutExtras) {
    auto setup = CreateSetup("DescribeOpPlain");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_plain";
    CreateTopic(runtime, path);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response->TopicInfo.Info);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 0u);
    UNIT_ASSERT(response->SelfEntry.name());
}

Y_UNIT_TEST(SuccessWithLocation) {
    auto setup = CreateSetup("DescribeOpLocation");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_location";
    CreateTopic(runtime, path, /*partitions=*/2);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 2u);
    for (const auto& [partitionId, info] : response->Partitions) {
        Y_UNUSED(partitionId);
        UNIT_ASSERT_GT(info.Location.node_id(), 0);
    }
}

Y_UNIT_TEST(SuccessWithLocationAndStats) {
    auto setup = CreateSetup("DescribeOpStats");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_stats";
    CreateTopic(runtime, path);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true, /*includeStats=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .WithReadSessions = true,
            .WithStatus = true,
            .ConsumerName = "user",
        }));

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->ConsumerName, "user");
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 1u);
    const auto& info = response->Partitions.begin()->second;
    UNIT_ASSERT_GT(info.Location.node_id(), 0);
    UNIT_ASSERT(info.Stats.has_partition_stats());
    UNIT_ASSERT(info.Stats.has_partition_consumer_stats());
}

Y_UNIT_TEST(MissingTopic) {
    auto setup = CreateSetup("DescribeOpMissing");
    auto& runtime = setup->GetRuntime();

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings("/Root/missing_topic_describe_op"),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(!response->ErrorMessage.empty());
}

Y_UNIT_TEST(RetriesWithSyncOnStaleSchemaValidation) {
    auto setup = CreateSetup("DescribeOpRetrySync");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_retry_sync";
    CreateTopic(runtime, path);

    ui32 validateCalls = 0;
    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .ValidateError = TDescribeSchemaError{
                .Status = Ydb::StatusIds::BAD_REQUEST,
                .Message = "missing in stale cache",
                .RetryWithSync = true,
            },
            .FailValidateTimes = 1,
            .ValidateCalls = &validateCalls,
        }));

    UNIT_ASSERT_VALUES_EQUAL(validateCalls, 2u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response->UsedSyncVersion);
}

Y_UNIT_TEST(DoesNotRetryValidateErrorWithoutRetryWithSync) {
    auto setup = CreateSetup("DescribeOpNoRetrySync");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_no_retry_sync";
    CreateTopic(runtime, path);

    ui32 validateCalls = 0;
    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .ValidateError = TDescribeSchemaError{
                .Status = Ydb::StatusIds::BAD_REQUEST,
                .Message = "hard validation error",
            },
            .ValidateCalls = &validateCalls,
        }));

    UNIT_ASSERT_VALUES_EQUAL(validateCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "hard validation error");
}

Y_UNIT_TEST(StrategyValidateError) {
    auto setup = CreateSetup("DescribeOpValidate");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_validate";
    CreateTopic(runtime, path);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .ValidateError = TDescribeSchemaError{
                .Status = Ydb::StatusIds::BAD_REQUEST,
                .Message = "strategy rejected schema",
                .IssueCode = Ydb::PersQueue::ErrorCode::VALIDATION_ERROR,
            },
        }));

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "strategy rejected schema");
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<int>(response->IssueCode),
        static_cast<int>(Ydb::PersQueue::ErrorCode::VALIDATION_ERROR));
}

Y_UNIT_TEST(FiltersPartitionsByStrategy) {
    auto setup = CreateSetup("DescribeOpFilter");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_describe_op_filter";
    CreateTopic(runtime, path, /*partitions=*/3);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .OnlyPartitionId = 1,
        }));

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 1u);
    UNIT_ASSERT(response->Partitions.contains(1));
}

Y_UNIT_TEST(RetriesOnLocationDeliveryProblem) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_op_retry";
    CreateTopic(runtime, path);

    size_t broken = 0;
    auto breakObserver = BreakFirstLocationForward(runtime, broken);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 1u);
    UNIT_ASSERT_GT(response->Partitions.begin()->second.Location.node_id(), 0);
}

Y_UNIT_TEST(RetriesOnEmptyStatusResponse) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_op_stats_retry";
    CreateTopic(runtime, path);

    size_t injected = 0;
    auto injectObserver = InjectEmptyStatusOnce(runtime, injected);

    auto response = RunDescribeOperation(
        runtime,
        MakeSettings(path, /*includeLocation=*/true, /*includeStats=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .WithReadSessions = true,
            .WithStatus = true,
            .ConsumerName = "user",
        }));

    UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response->Partitions.begin()->second.Stats.has_partition_stats());
}

Y_UNIT_TEST(TimesOutWhenLocationStuck) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_op_timeout";
    CreateTopic(runtime, path);

    auto dropObserver = DropLocationForwards(runtime);

    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeOperationActor(
        edge,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>())));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    auto handle = runtime.GrabEdgeEvent<TEvDescribeOperationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    auto response = THolder(handle->Release());
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::TIMEOUT);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "Describe request timed out");
}

Y_UNIT_TEST(PoisonRepliesCancelled) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    const TString path = "/Root/topic_describe_op_poison";
    CreateTopic(runtime, path);

    auto dropObserver = DropLocationForwards(runtime);

    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeOperationActor(
        edge,
        MakeSettings(path, /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>())));

    runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));
    runtime.Send(new IEventHandle(schedule.GetRoot(), edge, new NActors::TEvents::TEvPoison()));

    auto handle = runtime.GrabEdgeEvent<TEvDescribeOperationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    auto response = THolder(handle->Release());
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::CANCELLED);
}

} // Y_UNIT_TEST_SUITE(DescribeOperationActor)

} // namespace NKikimr::NPQ::NSchema
