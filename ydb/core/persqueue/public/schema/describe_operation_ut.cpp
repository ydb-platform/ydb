#include "schema_ut_helpers.h"

#include "describe_operation.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <optional>

namespace NKikimr::NPQ::NSchema {

using namespace NTests;

namespace {

using TNavigate = NSchemeCache::TSchemeCacheNavigate;

constexpr ui64 BALANCER_TABLET = 1001;
constexpr ui64 PARTITION_TABLET = 2001;
constexpr ui32 PARTITION_ID = 0;
constexpr ui32 LOCATION_NODE_ID = 7;
constexpr ui32 LOCATION_GENERATION = 3;

struct TIsolatedPipeConfig {
    enum class ELocation { Reply, Drop, FailOnce, StatusFalseAlways };
    enum class EStatus { Reply, EmptyOnce, FailAlways, WithConsumers };

    ELocation Location = ELocation::Reply;
    EStatus Status = EStatus::Reply;
};

enum class EFakeNavigateKind {
    Topic,
    Table,
};

struct TIsolatedPipeStats {
    size_t LocationForwards = 0;
    size_t StatusForwards = 0;
};

class TFakeSchemeCacheActor : public NActors::TActorBootstrapped<TFakeSchemeCacheActor> {
public:
    explicit TFakeSchemeCacheActor(EFakeNavigateKind kind = EFakeNavigateKind::Topic)
        : Kind(kind)
    {
    }

    void Bootstrap() {
        Become(&TFakeSchemeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
    )

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        auto request = std::move(ev->Get()->Request);
        for (auto& entry : request->ResultSet) {
            entry.Status = TNavigate::EStatus::Ok;
            if (Kind == EFakeNavigateKind::Table) {
                entry.Kind = TNavigate::EKind::KindTable;
                auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
                self->Info.SetName("table");
                self->Info.SetPathType(NKikimrSchemeOp::EPathTypeTable);
                entry.Self = self;
                continue;
            }

            entry.Kind = TNavigate::EKind::KindTopic;

            auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
            pqInfo->Description.SetBalancerTabletID(BALANCER_TABLET);
            auto* partition = pqInfo->Description.AddPartitions();
            partition->SetPartitionId(PARTITION_ID);
            partition->SetTabletId(PARTITION_TABLET);
            entry.PQGroupInfo = pqInfo;

            auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
            self->Info.SetName("topic");
            self->Info.SetPathType(NKikimrSchemeOp::EPathTypePersQueueGroup);
            entry.Self = self;
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    EFakeNavigateKind Kind;
};

class TFakePipeCacheActor : public NActors::TActorBootstrapped<TFakePipeCacheActor> {
public:
    TFakePipeCacheActor(TIsolatedPipeConfig config, TIsolatedPipeStats* stats)
        : Config(config)
        , Stats(stats)
    {
    }

    void Bootstrap() {
        Become(&TFakePipeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPipeCache::TEvForward, Handle);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
    )

private:
    void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
        if (!ev->Get()->Ev) {
            return;
        }
        const auto type = ev->Get()->Ev->Type();
        const ui64 tabletId = ev->Get()->TabletId;
        const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;

        if (type == TEvPersQueue::TEvGetPartitionsLocation::EventType) {
            ++Stats->LocationForwards;
            if (Config.Location == TIsolatedPipeConfig::ELocation::Drop) {
                return;
            }
            if (Config.Location == TIsolatedPipeConfig::ELocation::FailOnce && Stats->LocationForwards == 1) {
                Send(ev->Sender, new TEvPipeCache::TEvDeliveryProblem(tabletId, true), 0, subscribeCookie);
                return;
            }
            auto* response = new TEvPersQueue::TEvGetPartitionsLocationResponse();
            if (Config.Location == TIsolatedPipeConfig::ELocation::StatusFalseAlways) {
                response->Record.SetStatus(false);
                Send(ev->Sender, response, 0, ev->Cookie);
                return;
            }
            response->Record.SetStatus(true);
            auto* location = response->Record.AddLocations();
            location->SetPartitionId(PARTITION_ID);
            location->SetNodeId(LOCATION_NODE_ID);
            location->SetGeneration(LOCATION_GENERATION);
            Send(ev->Sender, response, 0, ev->Cookie);
            return;
        }

        if (type == TEvPersQueue::TEvStatus::EventType) {
            ++Stats->StatusForwards;
            if (Config.Status == TIsolatedPipeConfig::EStatus::EmptyOnce && Stats->StatusForwards == 1) {
                Send(ev->Sender, new TEvPersQueue::TEvStatusResponse(), 0, ev->Cookie);
                return;
            }
            if (Config.Status == TIsolatedPipeConfig::EStatus::FailAlways) {
                Send(ev->Sender, new TEvPersQueue::TEvStatusResponse(), 0, ev->Cookie);
                return;
            }
            auto* response = new TEvPersQueue::TEvStatusResponse();
            auto* part = response->Record.AddPartResult();
            part->SetPartition(PARTITION_ID);
            part->SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
            part->SetStartOffset(0);
            part->SetEndOffset(10);
            if (Config.Status == TIsolatedPipeConfig::EStatus::WithConsumers) {
                auto* cons = part->AddConsumerResult();
                cons->SetConsumer("user");
                cons->SetLastReadTimestampMs(1000);
                cons->SetReadLagMs(10);
                cons->SetWriteLagMs(20);
                cons->SetCommitedLagMs(30);
                cons->SetAvgReadSpeedPerMin(1);
                cons->SetAvgReadSpeedPerHour(2);
                cons->SetAvgReadSpeedPerDay(3);
            }
            Send(ev->Sender, response, 0, ev->Cookie);
            return;
        }

        if (type == TEvPersQueue::TEvGetReadSessionsInfo::EventType) {
            Send(ev->Sender, new TEvPersQueue::TEvReadSessionsInfoResponse(), 0, ev->Cookie);
        }
    }

    TIsolatedPipeConfig Config;
    TIsolatedPipeStats* Stats;
};

struct TIsolatedDescribeEnv {
    TIsolatedPipeStats PipeStats;
    NActors::TTestBasicRuntime Runtime;

    explicit TIsolatedDescribeEnv(
        TIsolatedPipeConfig config = {},
        EFakeNavigateKind navigateKind = EFakeNavigateKind::Topic)
        : Runtime(1, false)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.UpdateCurrentTime(TInstant::Now());
        Runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
        Runtime.SetLogPriority(NKikimrServices::PQ_SCHEMA, NActors::NLog::PRI_DEBUG);
        Runtime.SetLogPriority(NKikimrServices::PQ_DESCRIBER, NActors::NLog::PRI_DEBUG);

        auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(navigateKind));
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        auto pipeCacheId = Runtime.Register(new TFakePipeCacheActor(config, &PipeStats));
        Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheId);
        Runtime.DispatchEvents();
    }
};

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
    TIsolatedDescribeEnv env({.Location = TIsolatedPipeConfig::ELocation::FailOnce});

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic", /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(env.PipeStats.LocationForwards, 2u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.begin()->second.Location.node_id(), LOCATION_NODE_ID);
}

Y_UNIT_TEST(RetriesOnEmptyStatusResponse) {
    TIsolatedDescribeEnv env({.Status = TIsolatedPipeConfig::EStatus::EmptyOnce});

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic", /*includeLocation=*/true, /*includeStats=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .WithReadSessions = true,
            .WithStatus = true,
            .ConsumerName = "user",
        }));

    UNIT_ASSERT_VALUES_EQUAL(env.PipeStats.StatusForwards, 2u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response->Partitions.begin()->second.Stats.has_partition_stats());
}

Y_UNIT_TEST(TimesOutWhenLocationStuck) {
    TIsolatedDescribeEnv env({.Location = TIsolatedPipeConfig::ELocation::Drop});
    auto& runtime = env.Runtime;

    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeOperationActor(
        edge,
        MakeSettings("/Root/topic", /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>())));

    TDispatchOptions options;
    options.CustomFinalCondition = [&] {
        return env.PipeStats.LocationForwards >= 1;
    };
    runtime.DispatchEvents(options);
    runtime.AdvanceCurrentTime(TDuration::Seconds(31));

    auto handle = runtime.GrabEdgeEvent<TEvDescribeOperationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    auto response = THolder(handle->Release());
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::TIMEOUT);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "Describe request timed out");
}

Y_UNIT_TEST(PoisonRepliesCancelled) {
    TIsolatedDescribeEnv env({.Location = TIsolatedPipeConfig::ELocation::Drop});
    auto& runtime = env.Runtime;

    const auto edge = runtime.AllocateEdgeActor();
    TEnableScheduleForRootGuard schedule(runtime);
    schedule.SetRoot(runtime.Register(CreateDescribeOperationActor(
        edge,
        MakeSettings("/Root/topic", /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>())));

    TDispatchOptions options;
    options.CustomFinalCondition = [&] {
        return env.PipeStats.LocationForwards >= 1;
    };
    runtime.DispatchEvents(options);
    runtime.Send(new IEventHandle(schedule.GetRoot(), edge, new NActors::TEvents::TEvPoison()));

    auto handle = runtime.GrabEdgeEvent<TEvDescribeOperationResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handle);
    auto response = THolder(handle->Release());
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::CANCELLED);
}

Y_UNIT_TEST(BalancerRetriesExhaustedOnFalseStatus) {
    TIsolatedDescribeEnv env({.Location = TIsolatedPipeConfig::ELocation::StatusFalseAlways});

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic", /*includeLocation=*/true),
        std::make_unique<TTestDescribeStrategy>(),
        TDuration::Seconds(60));

    UNIT_ASSERT_GT(env.PipeStats.LocationForwards, 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::UNAVAILABLE);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "Partition locations are not available");
}

Y_UNIT_TEST(StatsRetriesExhaustedOnEmptyStatus) {
    TIsolatedDescribeEnv env({.Status = TIsolatedPipeConfig::EStatus::FailAlways});

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic", /*includeLocation=*/true, /*includeStats=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .WithReadSessions = true,
            .WithStatus = true,
            .ConsumerName = "user",
        }),
        TDuration::Seconds(60));

    UNIT_ASSERT_GT(env.PipeStats.StatusForwards, 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::UNAVAILABLE);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "unresponsive");
}

Y_UNIT_TEST(ValidateSchemaThrowsMapsToInternalError) {
    TIsolatedDescribeEnv env;

    class TThrowingStrategy: public IDescribeStrategy {
    public:
        TString GetName() const override { return "Throw"; }
        TDescribeSchemaResult ValidateSchema(const NDescriber::TTopicInfo&) override {
            throw yexception() << "boom";
        }
        bool NeedProcessPartition(
            const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition&) const override {
            return true;
        }
        std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const override {
            return nullptr;
        }
        std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const override {
            return nullptr;
        }
    };

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic"),
        std::make_unique<TThrowingStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "Unhandled exception");
}

Y_UNIT_TEST(NotTopicMapsToSchemeError) {
    TIsolatedDescribeEnv env({}, EFakeNavigateKind::Table);

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/table"),
        std::make_unique<TTestDescribeStrategy>());

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_EQUAL(response->IssueCode, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
}

Y_UNIT_TEST(StatusIncludesPerConsumerStats) {
    TIsolatedDescribeEnv env({.Status = TIsolatedPipeConfig::EStatus::WithConsumers});

    auto response = RunDescribeOperation(
        env.Runtime,
        MakeSettings("/Root/topic", /*includeLocation=*/true, /*includeStats=*/true),
        std::make_unique<TTestDescribeStrategy>(TTestDescribeStrategyOptions{
            .WithReadSessions = true,
            .WithStatus = true,
            .ConsumerName = "user",
        }));

    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Partitions.size(), 1u);
    UNIT_ASSERT(response->Partitions.begin()->second.Consumers.contains("user"));
}

} // Y_UNIT_TEST_SUITE(DescribeOperationActor)

} // namespace NKikimr::NPQ::NSchema
