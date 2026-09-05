#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/set_offsets/set_offsets.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <memory>

using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NSetOffsets;
using NKikimr::TEvPQ;
using NActors::IEventHandle;
using NActors::TActorId;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

namespace {

using TCoreSettings = NKikimr::NPQ::NSetOffsets::TSetOffsetsSettings;

std::atomic<ui64> TopicSeq{0};
std::unique_ptr<TTopicSdkTestSetup> ClusterInstance;

TString UniqueName(TStringBuf prefix) {
    return TStringBuilder() << prefix << TopicSeq.fetch_add(1);
}

TTopicSdkTestSetup& Cluster() {
    if (!ClusterInstance) {
        ClusterInstance = std::make_unique<TTopicSdkTestSetup>(
            "SetOffsetsActorTests", TTopicSdkTestSetup::MakeServerSettings(), false);
        // Default TTestServer logs PERSQUEUE at DEBUG (idle persist every ~100ms).
        // Under ASAN that races with KQP compile and blows up the last test in the chunk.
        ClusterInstance->GetServer().EnableLogs(
            ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_ERROR);
    }
    return *ClusterInstance;
}

void ShutdownCluster() {
    ClusterInstance.reset();
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

THolder<TEvSetOffsetsResult> WaitResult(
    NActors::TTestActorRuntime& runtime,
    const TRegisteredActor& actor,
    TDuration timeout = TDuration::Seconds(30))
{
    auto ev = runtime.GrabEdgeEvent<TEvSetOffsetsResult>(actor.Edge, timeout);
    UNIT_ASSERT_C(ev, "TEvSetOffsetsResult timed out");
    return THolder<TEvSetOffsetsResult>(ev->Release().Release());
}

void AssertRequestError(
    NActors::TTestActorRuntime& runtime,
    const TRegisteredActor& actor,
    Ydb::StatusIds::StatusCode status,
    const TString& substring)
{
    auto result = WaitResult(runtime, actor);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, status, result->Error);
    UNIT_ASSERT_STRING_CONTAINS(result->Error, substring);
}

void AssertAllPartitionsSuccess(const THolder<TEvSetOffsetsResult>& result) {
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

ui64 DescribeTabletId(TTopicSdkTestSetup& setup, const TString& topic) {
    auto edge = setup.GetRuntime().AllocateEdgeActor();
    NDescriber::TDescribeSettings describeSettings;
    setup.GetRuntime().Register(NDescriber::CreateDescriberActor(
        edge, "/Root", { TString{setup.GetFullTopicPath(topic)} }, describeSettings));
    auto described = setup.GetRuntime().GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(described);
    const auto& describedTopic = described->Get()->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(describedTopic.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT(describedTopic.Info);
    return describedTopic.Info->Description.GetPartitions(0).GetTabletId();
}

} // namespace

class TSetOffsetsActorSuite: public TTestBase {
public:
    void GlobalSuiteTearDown() override {
        // Destroy while the unittest runtime is still alive. A leaked TTestServer
        // keeps KQP compiling into process-exit teardown (ASAN heap-use-after-free).
        ShutdownCluster();
    }
};

Y_UNIT_TEST_SUITE_IMPL(TSetOffsetsActorTests, TSetOffsetsActorSuite) {

Y_UNIT_TEST(TopicNotExists) {
    auto& setup = Cluster();
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic_not_exists",
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertRequestError(runtime, actor, Ydb::StatusIds::SCHEME_ERROR, "does not exist");
}



Y_UNIT_TEST(TopicWithoutConsumer) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "other-consumer");
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer_not_exists",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertRequestError(runtime, actor, Ydb::StatusIds::SCHEME_ERROR, "does not exist");
}



Y_UNIT_TEST(Unauthorized) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
    setup.GetServer().AnnoyingClient->ModifyACL("/Root", topic, acl.SerializeAsString());

    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
        .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
    });
    auto result = WaitResult(runtime, actor);
    UNIT_ASSERT(result->Status == Ydb::StatusIds::SCHEME_ERROR || result->Status == Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(!result->Error.empty());
}



Y_UNIT_TEST(MlpConsumerRejected) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    TTopicClient client(setup.MakeDriver());
    auto status = client.CreateTopic(setup.GetFullTopicPath(topic), TCreateTopicSettings()
        .BeginAddSharedConsumer("mlp-consumer")
        .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    setup.GetServer().WaitInit(setup.GetTopicPath(topic));

    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "mlp-consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertRequestError(runtime, actor, Ydb::StatusIds::BAD_REQUEST, "MLP");
}



Y_UNIT_TEST(EmptyTopicEarliest) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 0);
}



Y_UNIT_TEST(EmptyTopicLatest) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::LATEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 0);
}



Y_UNIT_TEST(ManyPartitions) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer", 4);
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::LATEST,
    });
    auto result = WaitResult(runtime, actor);
    AssertAllPartitionsSuccess(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Partitions.size(), 4);
}



Y_UNIT_TEST(RewindActiveAfterWrite) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    setup.Write(topic, "m1", 0);
    setup.Write(topic, "m2", 0);

    auto client = setup.MakeClient();
    UNIT_ASSERT(client.CommitOffset(setup.GetFullTopicPath(topic), 0, "consumer", 2).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 2);

    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 0);
}



Y_UNIT_TEST(SkipToEnd) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    setup.Write(topic, "m1", 0);
    setup.Write(topic, "m2", 0);

    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::LATEST,
    });
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 2);
}



Y_UNIT_TEST(TimestampBeforeInsideAfter) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    const auto before = TInstant::Now() - TDuration::Hours(1);
    setup.Write(topic, "m1", 0);
    // Cross a second boundary so second-precision skip-obsolete-timestamp still
    // distinguishes m1 from the target (and so both messages can share a blob).
    const auto afterM1Second = TInstant::Seconds(TInstant::Now().Seconds() + 1);
    while (TInstant::Now() < afterM1Second) {
        Sleep(TDuration::MilliSeconds(10));
    }
    const auto middle = TInstant::Now();
    Sleep(TDuration::MilliSeconds(50));
    setup.Write(topic, "m2", 0);
    const auto after = TInstant::Now() + TDuration::Hours(1);

    auto& runtime = setup.GetRuntime();
    auto resetAt = [&](TInstant ts) {
        auto actor = CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = TString{setup.GetFullTopicPath(topic)},
            .Consumer = "consumer",
            .Position = NKikimrPQ::TEvSetOffsetsRequest::FROM_WRITTEN_AT,
            .TimestampMs = ts.MilliSeconds(),
        });
        AssertAllPartitionsSuccess(WaitResult(runtime, actor));
        return GetCommittedOffset(setup, topic, "consumer");
    };

    UNIT_ASSERT_VALUES_EQUAL(resetAt(before), 0);
    UNIT_ASSERT_VALUES_EQUAL(resetAt(middle), 1);
    UNIT_ASSERT_VALUES_EQUAL(resetAt(after), 2);
}



Y_UNIT_TEST(StaleCookieIgnored) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer", 1);
    auto& runtime = setup.GetRuntime();
    auto actor = CreateActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = TString{setup.GetFullTopicPath(topic)},
        .Consumer = "consumer",
        .Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST,
    });
    runtime.Send(new IEventHandle(actor.Actor, TActorId(), new TEvPQ::TEvSetOffsetsResponse(0, Ydb::StatusIds::GENERIC_ERROR, "stale", 999)));
    AssertAllPartitionsSuccess(WaitResult(runtime, actor));
}



Y_UNIT_TEST(TabletDirectEarliestLatest) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    setup.Write(topic, "m1", 0);

    const ui64 tabletId = DescribeTabletId(setup, topic);
    const TString path = TString{setup.GetFullTopicPath(topic)};
    auto edge = setup.GetRuntime().AllocateEdgeActor();

    NKikimr::ForwardToTablet(setup.GetRuntime(), tabletId, edge,
        new TEvPQ::TEvSetOffsetsRequest(path, "consumer", 0, NKikimrPQ::TEvSetOffsetsRequest::LATEST));
    {
        auto latest = setup.GetRuntime().GrabEdgeEvent<TEvPQ::TEvSetOffsetsResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(latest);
        UNIT_ASSERT_VALUES_EQUAL(latest->Get()->GetStatus(), Ydb::StatusIds::SUCCESS);
    }
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 1);

    NKikimr::ForwardToTablet(setup.GetRuntime(), tabletId, edge,
        new TEvPQ::TEvSetOffsetsRequest(path, "consumer", 0, NKikimrPQ::TEvSetOffsetsRequest::EARLIEST));
    {
        auto earliest = setup.GetRuntime().GrabEdgeEvent<TEvPQ::TEvSetOffsetsResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(earliest);
        UNIT_ASSERT_VALUES_EQUAL(earliest->Get()->GetStatus(), Ydb::StatusIds::SUCCESS);
    }
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 0);
}



Y_UNIT_TEST(TabletDirectUnspecifiedPosition) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");

    const ui64 tabletId = DescribeTabletId(setup, topic);
    const TString path = TString{setup.GetFullTopicPath(topic)};
    auto edge = setup.GetRuntime().AllocateEdgeActor();

    NKikimr::ForwardToTablet(setup.GetRuntime(), tabletId, edge,
        new TEvPQ::TEvSetOffsetsRequest(path, "consumer", 0, NKikimrPQ::TEvSetOffsetsRequest::POSITION_UNSPECIFIED));
    auto response = setup.GetRuntime().GrabEdgeEvent<TEvPQ::TEvSetOffsetsResponse>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->GetStatus(), Ydb::StatusIds::BAD_REQUEST);
}



Y_UNIT_TEST(TabletDirectUnknownPartition) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");

    const ui64 tabletId = DescribeTabletId(setup, topic);
    const TString path = TString{setup.GetFullTopicPath(topic)};
    auto edge = setup.GetRuntime().AllocateEdgeActor();

    NKikimr::ForwardToTablet(setup.GetRuntime(), tabletId, edge,
        new TEvPQ::TEvSetOffsetsRequest(path, "consumer", 999, NKikimrPQ::TEvSetOffsetsRequest::EARLIEST, 0, 42));
    auto response = setup.GetRuntime().GrabEdgeEvent<TEvPQ::TEvSetOffsetsResponse>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(response->Get()->GetErrorMessage(), "not found");
}



Y_UNIT_TEST(TabletDirectResetDoesNotStealCommit) {
    auto& setup = Cluster();
    const auto topic = UniqueName("topic_");
    setup.CreateTopic(topic, "consumer");
    setup.Write(topic, "m1", 0);

    auto client = setup.MakeClient();
    const TString path = TString{setup.GetFullTopicPath(topic)};
    UNIT_ASSERT(client.CommitOffset(path, 0, "consumer", 1).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 1);

    const ui64 tabletId = DescribeTabletId(setup, topic);
    auto edge = setup.GetRuntime().AllocateEdgeActor();
    NKikimr::ForwardToTablet(setup.GetRuntime(), tabletId, edge,
        new TEvPQ::TEvSetOffsetsRequest(path, "consumer", 0, NKikimrPQ::TEvSetOffsetsRequest::EARLIEST, 0, 1));
    {
        auto reset = setup.GetRuntime().GrabEdgeEvent<TEvPQ::TEvSetOffsetsResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(reset);
        UNIT_ASSERT_VALUES_EQUAL(reset->Get()->GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(reset->Get()->GetCookie(), 1u);
    }
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 0);

    UNIT_ASSERT(client.CommitOffset(path, 0, "consumer", 1).GetValueSync().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, topic, "consumer"), 1);
}

} // TSetOffsetsActorTests
