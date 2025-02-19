#include <ydb/core/persqueue/events/internal.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/core/persqueue/list_all_topics_actor.h>


namespace NKikimr::NPersQueueTests {

Y_UNIT_TEST_SUITE(TListAllTopicsTests) {
    TString db = "/Root";

    THolder<TEvPQ::TEvListAllTopicsResponse> GetListing(
                TTestActorRuntime* runtime, bool recursive,const TMaybe<ui64>& limit = {}, const TString& startFrom = {}
    ) {
        auto edge = runtime->AllocateEdgeActor();
        runtime->Register(NKikimr::NPersQueue::MakeListAllTopicsActor(
                    edge, db, "", recursive, startFrom, limit));


        auto resp = runtime->GrabEdgeEvent<TEvPQ::TEvListAllTopicsResponse>(TDuration::Seconds(10));
        return resp;
    }

    void CreateTopic(auto& pqClient, const TString& topicName, bool wait) {
        auto settings = NYdb::NPersQueue::TCreateTopicSettings().PartitionsCount(1);


            auto createF = pqClient.CreateTopic(topicName, settings);
            if (!wait)
                return;
            auto createRes = createF.GetValueSync();
            UNIT_ASSERT_C(createRes.IsSuccess(), createRes.GetIssues().ToString());
    }

    Y_UNIT_TEST(PlainList) {
        auto settings = PQSettings(0, 1);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        ::NPersQueue::TTestServer server{settings, true};

        auto* runtime = server.GetRuntime();
        auto port = server.GrpcPort;

        TString endpoint = TStringBuilder() << "localhost:" << port;
        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            ;
            //.SetDatabase(db);
            //.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
        auto driver = MakeHolder<NYdb::TDriver>(driverConfig);
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*driver);

        CreateTopic(pqClient, NKikimr::JoinPath({db, "topic1"}), false);
        CreateTopic(pqClient, NKikimr::JoinPath({db, "topic2"}), true);

        auto resp = GetListing(runtime, true);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 2);
        resp = GetListing(runtime, false);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 2);

        CreateTopic(pqClient, NKikimr::JoinPath({db, "topic3"}), true);
        resp = GetListing(runtime, false);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 3);
    }

    Y_UNIT_TEST(RecursiveList) {
        auto settings = PQSettings(0, 1);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        ::NPersQueue::TTestServer server{settings, true};

        auto* runtime = server.GetRuntime();
        auto port = server.GrpcPort;

        TString endpoint = TStringBuilder() << "localhost:" << port;
        TString db = "/Root";
        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            ;
            //.SetDatabase(db);
            //.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
        auto driver = MakeHolder<NYdb::TDriver>(driverConfig);
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*driver);

        CreateTopic(pqClient, NKikimr::JoinPath({db, "topic1"}), false);
        server.AnnoyingClient->MkDir("/Root", "dir1");
        server.AnnoyingClient->MkDir("/Root", "dir2");
        CreateTopic(pqClient, NKikimr::JoinPath({db, "dir1", "topic2"}), false);
        CreateTopic(pqClient, NKikimr::JoinPath({db, "dir2", "topic3"}), true);

        auto resp = GetListing(runtime, true);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 3);
        resp = GetListing(runtime, false);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 1);
    }

    Y_UNIT_TEST(ListLimitAndPaging) {
        auto settings = PQSettings(0, 1);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        ::NPersQueue::TTestServer server{settings, true};

        auto* runtime = server.GetRuntime();
        auto port = server.GrpcPort;

        TString endpoint = TStringBuilder() << "localhost:" << port;
        TString db = "/Root";
        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            ;
            //.SetDatabase(db);
            //.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
        auto driver = MakeHolder<NYdb::TDriver>(driverConfig);
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*driver);

        CreateTopic(pqClient, NKikimr::JoinPath({db, "topic1"}), false);
        server.AnnoyingClient->MkDir("/Root", "dir1");
        server.AnnoyingClient->MkDir("/Root", "dir2");
        CreateTopic(pqClient, NKikimr::JoinPath({db, "dir1", "topic2"}), false);
        CreateTopic(pqClient, NKikimr::JoinPath({db, "dir2", "topic3"}), true);

        auto resp = GetListing(runtime, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 1);
        resp = GetListing(runtime, true, 2);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 2);
        resp = GetListing(runtime, true, 3);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 3);
        resp = GetListing(runtime, true, Nothing(), NKikimr::JoinPath({"dir2", "topic3"}));
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 1);
    }
};

} // namespace
