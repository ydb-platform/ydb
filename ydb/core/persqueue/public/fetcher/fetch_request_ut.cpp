#include "fetch_request_actor.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>

namespace NKikimr::NPQ {
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

namespace {

constexpr ui64 RlNoResourceTag = 3;

void StartSchemeCache(TTestActorRuntime& runtime) {
    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        auto* appData = &runtime.GetAppData(nodeIndex);
        auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, new ::NMonitoring::TDynamicCounters());
        IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
        TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
        runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
    }
}

void EnableFetchLogs(TTopicSdkTestSetup& setup) {
    setup.GetServer().EnableLogs(
        {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST},
        NActors::NLog::PRI_DEBUG);
}

void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    TDriver driver(setup.MakeDriverConfig());
    TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();

    Cerr << "DDL: " << query << Endl << Flush;
    auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    driver.Stop(true);
}

TFetchRequestSettings MakeSettings(
    TVector<TPartitionFetchRequest> partitions,
    ui64 maxWaitTimeMs = 1000,
    ui64 totalMaxBytes = 1_MB)
{
    return TFetchRequestSettings{
        .Database = {},
        .Consumer = CLIENTID_WITHOUT_CONSUMER,
        .Partitions = std::move(partitions),
        .MaxWaitTimeMs = maxWaitTimeMs,
        .TotalMaxBytes = totalMaxBytes,
    };
}

TActorId RegisterFetchActor(TTestActorRuntime& runtime, const TFetchRequestSettings& settings, const TActorId& edgeId) {
    auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
    runtime.EnableScheduleForActor(fetchId);
    return fetchId;
}

THolder<TEvPQ::TEvFetchResponse> Fetch(
    TTestActorRuntime& runtime,
    const TFetchRequestSettings& settings,
    TDuration timeout = TDuration::Seconds(30))
{
    auto edgeId = runtime.AllocateEdgeActor();
    RegisterFetchActor(runtime, settings, edgeId);
    runtime.DispatchEvents();
    auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(timeout);
    UNIT_ASSERT_C(ev, "Timed out waiting for TEvFetchResponse");
    return ev;
}

void AssertErrorCode(const NKikimrClient::TPersQueueFetchResponse::TPartResult& result,
                     ::NPersQueue::NErrorCode::EErrorCode expected)
{
    UNIT_ASSERT_VALUES_EQUAL(
        ::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
        ::NPersQueue::NErrorCode::EErrorCode_Name(expected));
}

NKikimr::Tests::TServerSettings MakeMeteringServerSettings() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.PQConfig.MutableBillingMeteringConfig()->SetEnabled(true);
    return settings;
}

void CreateTopicWithAttrs(
    TTopicSdkTestSetup& setup,
    const TString& path,
    ui32 partitionCount,
    const THashMap<TString, TString>& attributes = {},
    std::optional<EMeteringMode> meteringMode = std::nullopt)
{
    auto client = setup.MakeClient();
    TCreateTopicSettings settings;
    settings.PartitioningSettings(partitionCount, partitionCount);
    for (const auto& [key, value] : attributes) {
        settings.AddAttribute(key, value);
    }
    if (meteringMode) {
        settings.MeteringMode(*meteringMode);
    }
    auto status = client.CreateTopic(path, settings).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
}

ui64 GetPartitionTabletId(TTopicSdkTestSetup& setup, const TString& topicPath, ui32 partitionId) {
    auto describeResult = setup.GetServer().AnnoyingClient->Ls(topicPath);
    UNIT_ASSERT_C(describeResult->Record.GetPathDescription().HasPersQueueGroup(), describeResult->Record);
    const auto& pq = describeResult->Record.GetPathDescription().GetPersQueueGroup();
    for (const auto& p : pq.GetPartitions()) {
        if (p.GetPartitionId() == partitionId) {
            return p.GetTabletId();
        }
    }
    UNIT_FAIL("partition tablet not found");
    return 0;
}

} // namespace

Y_UNIT_TEST_SUITE(TFetchRequestTests) {

    // ---- Existing scenarios (rewritten) ----

    Y_UNIT_TEST(HappyWay) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 5);
        setup->CreateTopic("topic2", "dc1", 5);

        setup->Write("/Root/topic1", "Data 1-1", 1);
        setup->Write("/Root/topic1", "Data 1-2", 1);
        setup->Write("/Root/topic1", "Data 1-3", 1);

        setup->Write("/Root/topic2", "Data 2-1", 3);
        setup->Write("/Root/topic2", "Data 2-2", 3);

        auto settings = MakeSettings({
            {"/Root/topic1", 1, 1, 10000},
            {"/Root/topic2", 3, 0, 10000},
            {"/Root/topic2", 2, 1, 10000}, // offset > endOffset
        });

        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 3);

        {
            const auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::OK);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 3);
            UNIT_ASSERT_GE(result.GetReadResult().ResultSize(), 1);
        }
        {
            const auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 3);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::OK);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 2);
            UNIT_ASSERT_GE(result.GetReadResult().ResultSize(), 1);
        }
        {
            const auto& result = ev->Response.GetPartResult(2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 2);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
        }
    }

    Y_UNIT_TEST(CDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");
        ExecuteDDL(*setup, "INSERT INTO table1 (id) VALUES (1)");

        auto ev = Fetch(runtime, MakeSettings({{"/Root/table1/feed", 0, 0, 10000}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);

        const auto& result = ev->Response.GetPartResult(0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/table1/feed");
        UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
        AssertErrorCode(result, ::NPersQueue::NErrorCode::OK);
        UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
    }

    Y_UNIT_TEST(SmallBytesRead) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->Write("/Root/topic1", TString(2_KB, 'a'), 0);

        auto settings = MakeSettings(
            {{"/Root/topic1", 0, 0, 1_KB}, {"/Root/topic1", 1, 0, 1_KB}},
            /*maxWaitTimeMs=*/1000,
            /*totalMaxBytes=*/100);

        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        {
            const auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::OK);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetResult(0).GetUncompressedSize(), 2_KB);
        }
        {
            const auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::READ_NOT_DONE);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);

        auto settings = MakeSettings(
            {{"/Root/topic1", 0, 0, 1_KB}, {"/Root/topic1", 1, 0, 1_KB}},
            /*maxWaitTimeMs=*/100,
            /*totalMaxBytes=*/100);

        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        for (ui32 i = 0; i < 2; ++i) {
            const auto& result = ev->Response.GetPartResult(i);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), i);
            AssertErrorCode(result, ::NPersQueue::NErrorCode::READ_NOT_DONE);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(BadTopicName) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 5);

        auto settings = MakeSettings({
            {"/Root/topic1", 1, 1, 10000},
            {"/Root/topic2", 3, 0, 10000},
        });

        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SCHEME_ERROR, ev->Message);
    }

    Y_UNIT_TEST(CheckAccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        auto& runtime = setup->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::PQ_FETCH_REQUEST, NActors::NLog::EPriority::PRI_DEBUG);
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 5);

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
            setup->GetServer().AnnoyingClient->ModifyACL("/Root", "topic1", acl.SerializeAsString());
        }

        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};

        {
            auto settings = MakeSettings({p1}, /*maxWaitTimeMs=*/100, /*totalMaxBytes=*/10000);
            settings.UserToken = MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{});

            auto ev = Fetch(runtime, settings);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        }

        {
            auto settings = MakeSettings({p1}, /*maxWaitTimeMs=*/100, /*totalMaxBytes=*/10000);
            settings.UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});

            auto ev = Fetch(runtime, settings);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::UNAUTHORIZED, ev->Message);
        }
    }

    // ---- P0: request validation ----

    Y_UNIT_TEST(EmptyTopicInRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        auto ev = Fetch(runtime, MakeSettings({{"", 0, 0, 10000}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "Empty topic");
    }

    Y_UNIT_TEST(ZeroMaxBytes) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        auto ev = Fetch(runtime, MakeSettings({{"/Root/topic1", 0, 0, 0}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "No maxBytes");
    }

    Y_UNIT_TEST(DuplicatePartition) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        auto ev = Fetch(runtime, MakeSettings({
            {"/Root/topic1", 0, 0, 10000},
            {"/Root/topic1", 0, 1, 10000},
        }));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "multiple times");
    }

    Y_UNIT_TEST(UnknownPartition) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);

        auto ev = Fetch(runtime, MakeSettings({{"/Root/topic1", 42, 0, 10000}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "PQ12");
    }

    // ---- P1: runtime errors ----

    Y_UNIT_TEST(PipeDisconnect) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        // Empty topic: HasData stays pending until the tablet/pipe dies.
        setup->CreateTopic("topic1", "dc1", 1);
        const ui64 tabletId = GetPartitionTabletId(*setup, "/Root/topic1", 0);

        auto edgeId = runtime.AllocateEdgeActor();
        auto settings = MakeSettings({{"/Root/topic1", 0, 0, 10000}}, /*maxWaitTimeMs=*/30'000);
        RegisterFetchActor(runtime, settings, edgeId);

        // Advance runtime so describe/pipes/HasData are in flight, then kill the tablet.
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        setup->GetServer().AnnoyingClient->KillTablet(*setup->GetServer().CleverServer, tabletId);

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(TDuration::Seconds(30));
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::TABLET_PIPE_DISCONNECTED);
    }

    Y_UNIT_TEST(SessionInvalidated) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        // Empty partition keeps HasData pending so we can inject SessionInvalidated in StateWork.
        setup->CreateTopic("topic1", "dc1", 1);

        auto edgeId = runtime.AllocateEdgeActor();
        auto settings = MakeSettings({{"/Root/topic1", 0, 0, 10000}}, /*maxWaitTimeMs=*/30'000);
        auto fetchId = RegisterFetchActor(runtime, settings, edgeId);

        // Reach StateWork (HasData handlers) before injecting — StateDescribe rejects this event.
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));

        auto response = MakeHolder<TEvPersQueue::TEvHasDataInfoResponse>();
        response->Record.SetCookie(0);
        response->Record.SetSessionInvalidated(true);
        runtime.Send(new IEventHandle(fetchId, fetchId, response.Release()), 0, true);

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(TDuration::Seconds(30));
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::READ_ERROR_NO_SESSION);
    }

    Y_UNIT_TEST(TimeoutHasDataResend) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 1);

        // MaxWaitTimeMs path: empty partition must finish with READ_NOT_DONE after wait/timeout
        // (FinishProcessing may re-issue HasData with Deadline=0 before answering).
        const auto started = TInstant::Now();
        auto ev = Fetch(
            runtime,
            MakeSettings({{"/Root/topic1", 0, 0, 1_KB}}, /*maxWaitTimeMs=*/200, /*totalMaxBytes=*/100),
            TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::READ_NOT_DONE);
        UNIT_ASSERT_LT(TInstant::Now() - started, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(MultiPartitionSameTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->Write("/Root/topic1", "p0", 0);
        setup->Write("/Root/topic1", "p1", 1);

        auto ev = Fetch(runtime, MakeSettings({
            {"/Root/topic1", 0, 0, 10000},
            {"/Root/topic1", 1, 0, 10000},
        }));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        AssertErrorCode(ev->Response.GetPartResult(1), ::NPersQueue::NErrorCode::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetPartResult(0).GetReadResult().GetMaxOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetPartResult(1).GetReadResult().GetMaxOffset(), 1);
    }

    Y_UNIT_TEST(MaxWaitCapped30s) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 1);
        const ui64 tabletId = GetPartitionTabletId(*setup, "/Root/topic1", 0);

        // Actor caps wait at 30s. Pass 60s and ensure we can still finish quickly via pipe error
        // (i.e. we are not stuck for the full uncapped client wait).
        auto settings = MakeSettings({{"/Root/topic1", 0, 0, 1_KB}}, /*maxWaitTimeMs=*/60'000, /*totalMaxBytes=*/100);
        auto edgeId = runtime.AllocateEdgeActor();
        RegisterFetchActor(runtime, settings, edgeId);

        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        const auto killAt = TInstant::Now();
        setup->GetServer().AnnoyingClient->KillTablet(*setup->GetServer().CleverServer, tabletId);

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(TDuration::Seconds(30));
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_LT(TInstant::Now() - killAt, TDuration::Seconds(20));
    }

    // ---- P2: flags / RL / consumer ----

    Y_UNIT_TEST(ReadTimestampMsAndCanReadBatches) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 1);
        setup->Write("/Root/topic1", "data", 0);

        const ui64 readTs = 123456;
        TPartitionFetchRequest part{"/Root/topic1", 0, 0, 10000, readTs};
        auto settings = MakeSettings({part});
        settings.CanReadBatches = true;

        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        // ReadTimestampMs is reflected in CmdReadResult.ReadFromTimestampMs.
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetPartResult(0).GetReadResult().GetReadFromTimestampMs(), readTs);
    }

    Y_UNIT_TEST(TimestampType) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        CreateTopicWithAttrs(*setup, "/Root/topic1", 1, {{"_timestamp_type", "LogAppendTime"}});
        setup->Write("/Root/topic1", "data", 0);

        auto ev = Fetch(runtime, MakeSettings({{"/Root/topic1", 0, 0, 10000}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT(ev->Response.HasTimestampType());
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetTimestampType(), "LogAppendTime");
    }

    Y_UNIT_TEST(ConsumerPropagated) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        const TString consumer = "fetch-consumer";
        setup->CreateTopic("topic1", consumer, 1);
        setup->Write("/Root/topic1", "data", 0);

        auto settings = MakeSettings({{"/Root/topic1", 0, 0, 10000}});
        settings.Consumer = consumer;

        // Reading with a real consumer must succeed (ClientId is required by the tablet read path).
        auto ev = Fetch(runtime, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        UNIT_ASSERT_GE(ev->Response.GetPartResult(0).GetReadResult().ResultSize(), 1);
    }

    Y_UNIT_TEST(PathCanonization) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 1);
        setup->Write("/Root/topic1", "data", 0);

        // Double slash must resolve via CanonizePath (actor + describer key alignment).
        auto ev = Fetch(runtime, MakeSettings({{"//Root/topic1", 0, 0, 10000}}));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
    }

    Y_UNIT_TEST(QuotaAllowedWithRlCtx) {
        auto settingsServer = MakeMeteringServerSettings();
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, settingsServer, false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        CreateTopicWithAttrs(*setup, "/Root/topic1", 2, {}, EMeteringMode::RequestUnits);
        setup->Write("/Root/topic1", "p0", 0);
        setup->Write("/Root/topic1", "p1", 1);

        // Non-existent RL path → acquire fails with non-TIMEOUT → fallback onSuccess (RlAllowed).
        // Functional check: multi-partition fetch completes under REQUEST_UNITS + RlCtx + RuPerRequest.
        auto settings = MakeSettings({
            {"/Root/topic1", 0, 0, 10000},
            {"/Root/topic1", 1, 0, 10000},
        });
        settings.RlCtx = TRlContext("/Root/nonexistent-coordination", "ru", "/Root", "");
        settings.RuPerRequest = true;

        auto ev = Fetch(runtime, settings, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        AssertErrorCode(ev->Response.GetPartResult(1), ::NPersQueue::NErrorCode::OK);
    }

    Y_UNIT_TEST(QuotaNoResourceThenAllowed) {
        auto settingsServer = MakeMeteringServerSettings();
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, settingsServer, false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        CreateTopicWithAttrs(*setup, "/Root/topic1", 2, {}, EMeteringMode::RequestUnits);
        setup->Write("/Root/topic1", "p0", 0);
        setup->Write("/Root/topic1", "p1", 1);

        auto edgeId = runtime.AllocateEdgeActor();
        auto settings = MakeSettings({
            {"/Root/topic1", 0, 0, 10000},
            {"/Root/topic1", 1, 0, 10000},
        });
        settings.RlCtx = TRlContext("/Root/nonexistent-coordination", "ru", "/Root", "");
        settings.RuPerRequest = true;

        auto fetchId = RegisterFetchActor(runtime, settings, edgeId);

        // Periodically inject RlNoResource while the fetch is running. When the actor is waiting
        // for quota this re-triggers RequestDataQuota; otherwise the wakeup is ignored safely.
        THolder<TEvPQ::TEvFetchResponse> ev;
        for (ui32 i = 0; i < 200 && !ev; ++i) {
            runtime.Send(new IEventHandle(fetchId, fetchId, new TEvents::TEvWakeup(RlNoResourceTag)), 0, true);
            runtime.DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(50));
            ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_C(ev, "Timed out waiting for TEvFetchResponse");
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        AssertErrorCode(ev->Response.GetPartResult(1), ::NPersQueue::NErrorCode::OK);
    }

    // ---- Lifecycle ----

    Y_UNIT_TEST(StaleReadResponseIgnored) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 1);
        setup->Write("/Root/topic1", "data", 0);

        auto edgeId = runtime.AllocateEdgeActor();
        auto settings = MakeSettings({{"/Root/topic1", 0, 0, 10000}});
        auto fetchId = RegisterFetchActor(runtime, settings, edgeId);

        // Reach StateWork, then inject a response with a foreign cookie / no active read.
        runtime.DispatchEvents(TDispatchOptions{}, TDuration::Seconds(1));
        {
            auto stale = MakeHolder<TEvPersQueue::TEvResponse>();
            stale->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
            auto* part = stale->Record.MutablePartitionResponse();
            part->SetCookie(999);
            runtime.Send(new IEventHandle(fetchId, fetchId, stale.Release()), 0, true);
        }

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>(TDuration::Seconds(30));
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
    }

};

} // namespace NKikimr::NPQ
