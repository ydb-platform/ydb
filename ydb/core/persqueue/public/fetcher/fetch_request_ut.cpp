#include "fetch_request_actor.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
using namespace ::NPersQueue;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

void EnableFetchLogs(TTopicSdkTestSetup& setup) {
    setup.GetServer().EnableLogs(
        {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST},
        NActors::NLog::PRI_DEBUG);
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


} // namespace

Y_UNIT_TEST_SUITE(TFetchRequestTests) {
    void StartSchemeCache(TTestActorRuntime& runtime) {
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            auto* appData = &runtime.GetAppData(nodeIndex);

            auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, new ::NMonitoring::TDynamicCounters());

            IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
            TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
            runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
        }
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

    Y_UNIT_TEST(HappyWay) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);
        setup->Write("/Root/topic1", "Data 1-1", 1);
        setup->Write("/Root/topic1", "Data 1-2", 1);
        setup->Write("/Root/topic1", "Data 1-3", 1);

        setup->CreateTopic("topic2", "dc1", totalPartitions);
        setup->Write("/Root/topic1", "Data 2-1", 3);
        setup->Write("/Root/topic1", "Data 2-2", 3);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"/Root/topic2", 3, 0, 10000};
        TPartitionFetchRequest pbad{"/Root/topic2", 2, 1, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1, p2, pbad}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        Cerr << "Got event: " << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 3);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 3);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 3);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
        }

        {
            auto& result = ev->Response.GetPartResult(2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 2);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_ERROR_TOO_BIG_OFFSET));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
        }
    }

    Y_UNIT_TEST(CDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");
        ExecuteDDL(*setup, "INSERT INTO table1 (id) VALUES (1)");


        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/table1/feed", 0, 0, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        Cerr << "Got event: " << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/table1/feed");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
        }
    }

    Y_UNIT_TEST(SmallBytesRead) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->Write("/Root/topic1", TString(2_KB, 'a'), 0);

        TPartitionFetchRequest p1 {"/Root/topic1", 0, 0, 1_KB};
        TPartitionFetchRequest p2 {"/Root/topic1", 1, 0, 1_KB};

        TFetchRequestSettings settings{
            .Database = {},
            .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
            .Partitions = {p1, p2},
            .MaxWaitTimeMs = 1000,
            .TotalMaxBytes = 100
        };

        auto edgeId = runtime.AllocateEdgeActor();
        auto fetchActorId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchActorId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        Cerr << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);

        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetResult(0).GetUncompressedSize(), 2_KB);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);

        TPartitionFetchRequest p1 {"/Root/topic1", 0, 0, 1_KB};
        TPartitionFetchRequest p2 {"/Root/topic1", 1, 0, 1_KB};

        TFetchRequestSettings settings{
            .Database = {},
            .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
            .Partitions = {p1, p2},
            .MaxWaitTimeMs = 100,
            .TotalMaxBytes = 100
        };

        auto edgeId = runtime.AllocateEdgeActor();
        auto fetchActorId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchActorId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        Cerr << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);

        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(BadTopicName) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_TRACE);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"/Root/topic2", 3, 0, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1, p2}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SCHEME_ERROR, ev->Message);
    }

    Y_UNIT_TEST(CheckAccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::PQ_FETCH_REQUEST, NActors::NLog::EPriority::PRI_DEBUG);
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
            setup->GetServer().AnnoyingClient->ModifyACL("/Root", "topic1", acl.SerializeAsString());

            auto goodToken = MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{});
            TFetchRequestSettings settings{
                .Database = {},
                .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
                .Partitions = {p1},
                .MaxWaitTimeMs = 100,
                .TotalMaxBytes = 10000,
                .RuPerRequest = false,
                .UserToken = goodToken
            };

            auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
            runtime.EnableScheduleForActor(fetchId);

            auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Status,Ydb::StatusIds::SUCCESS, ev->Message);
        }

        {
            auto badToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            TFetchRequestSettings settings{
                .Database = {},
                .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
                .Partitions = {p1},
                .MaxWaitTimeMs = 100,
                .TotalMaxBytes = 10000,
                .RuPerRequest = false,
                .UserToken = badToken
            };

            auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
            runtime.EnableScheduleForActor(fetchId);

            auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
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

    Y_UNIT_TEST(UnknownPartitionsThenValidTopicReturnsBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->CreateTopic("topic2", "dc1", 2);

        // Request order does not determine the order of metadata processing:
        // the actor stores topics in a hash map.
        auto ev = Fetch(runtime, MakeSettings({
            {"/Root/topic1", 42, 0, 10000},
            {"/Root/topic1", 43, 0, 10000},
            {"/Root/topic2", 0, 0, 10000},
            {"/Root/topic2", 1, 0, 10000},
        }));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "/Root/topic1");
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "PQ12");
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 0);
    }

    // Isolate a possible process abort if the double-PassAway regression returns (#58064).
    SIMPLE_UNIT_FORKED_TEST(UnknownPartitionsInTwoTopicsReturnsBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->CreateTopic("topic2", "dc1", 2);

        // Both topics are invalid, regardless of their hash-map iteration order.
        auto ev = Fetch(runtime, MakeSettings({
            {"/Root/topic1", 42, 0, 10000},
            {"/Root/topic2", 42, 0, 10000},
        }));
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::BAD_REQUEST, ev->Message);
        UNIT_ASSERT_STRING_CONTAINS(ev->Message, "PQ12");
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 0);
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

    Y_UNIT_TEST(ReadyPartitionIsNotBlockedByEmptyPartitionPastMaxWait) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableFetchLogs(*setup);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->Write("/Root/topic1", "ready", 0);

        auto ev = Fetch(
            runtime,
            MakeSettings({
                {"/Root/topic1", 0, 0, 1_KB},
                {"/Root/topic1", 1, 0, 1_KB},
            }, /*maxWaitTimeMs=*/200, /*totalMaxBytes=*/100),
            TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL_C(ev->Status, Ydb::StatusIds::SUCCESS, ev->Message);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);
        AssertErrorCode(ev->Response.GetPartResult(0), ::NPersQueue::NErrorCode::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetPartResult(0).GetReadResult().GetMaxOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.GetPartResult(0).GetReadResult().ResultSize(), 1);
        AssertErrorCode(ev->Response.GetPartResult(1), ::NPersQueue::NErrorCode::READ_NOT_DONE);
    }
};

} // namespace NKikimr::NPQ
