#include "list_all_topics_actor.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

using namespace NPersQueue;
using namespace NYdb::NTopic::NTests;
using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(TListAllTopicsTests) {

    void EnableLogs(TTopicSdkTestSetup& setup) {
        setup.GetServer().EnableLogs(
            {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PERSQUEUE},
            NActors::NLog::PRI_DEBUG
        );
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

    void MkDir(TTopicSdkTestSetup& setup, const TString& parent, const TString& name) {
        setup.GetServer().AnnoyingClient->MkDir(parent, name);
    }

    THolder<TEvPQ::TEvListAllTopicsResponse> GetListing(
        NActors::TTestActorRuntime& runtime,
        bool recursive,
        const TMaybe<ui64>& limit = {},
        const TString& startFrom = {},
        const TString& databasePath = "/Root",
        const TString& token = {})
    {
        auto edge = runtime.AllocateEdgeActor();
        runtime.Register(MakeListAllTopicsActor(edge, databasePath, token, recursive, startFrom, limit));
        auto resp = runtime.GrabEdgeEvent<TEvPQ::TEvListAllTopicsResponse>(TDuration::Seconds(10));
        UNIT_ASSERT_C(resp, "TEvListAllTopicsResponse timeout");
        return resp;
    }

    void AssertTopics(
        const THolder<TEvPQ::TEvListAllTopicsResponse>& resp,
        const TVector<TString>& expected,
        bool haveMore = false)
    {
        UNIT_ASSERT_VALUES_EQUAL(resp->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(resp->Error.empty());
        UNIT_ASSERT_VALUES_EQUAL(resp->HaveMoreTopics, haveMore);
        UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(resp->Topics[i], expected[i]);
        }
    }

    // -------------------------------------------------------------------------
    // P0 — core listing / recursive / paging / CDC
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(EmptyRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {});
    }

    Y_UNIT_TEST(FlatTopicsSortedRelativePaths) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic_b");
        ExecuteDDL(*setup, "CREATE TOPIC topic_a");

        auto resp = GetListing(setup->GetRuntime(), false);
        AssertTopics(resp, {"topic_a", "topic_b"});

        resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"topic_a", "topic_b"});
    }

    Y_UNIT_TEST(NonRecursiveIgnoresNested) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        MkDir(*setup, "/Root", "dir2");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");
        ExecuteDDL(*setup, "CREATE TOPIC `dir2/topic3`");

        auto resp = GetListing(setup->GetRuntime(), false);
        AssertTopics(resp, {"topic1"});
    }

    Y_UNIT_TEST(RecursiveListsNested) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        MkDir(*setup, "/Root", "dir2");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");
        ExecuteDDL(*setup, "CREATE TOPIC `dir2/topic3`");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"dir1/topic2", "dir2/topic3", "topic1"});
    }

    Y_UNIT_TEST(LimitSetsHaveMore) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        MkDir(*setup, "/Root", "dir2");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");
        ExecuteDDL(*setup, "CREATE TOPIC `dir2/topic3`");

        auto resp = GetListing(setup->GetRuntime(), true, 1);
        AssertTopics(resp, {"dir1/topic2"}, true);

        resp = GetListing(setup->GetRuntime(), true, 2);
        AssertTopics(resp, {"dir1/topic2", "dir2/topic3"}, true);
    }

    Y_UNIT_TEST(LimitExactNoMore) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");

        auto resp = GetListing(setup->GetRuntime(), true, 2);
        AssertTopics(resp, {"dir1/topic2", "topic1"}, false);
    }

    Y_UNIT_TEST(StartFromExclusive) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        MkDir(*setup, "/Root", "dir2");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");
        ExecuteDDL(*setup, "CREATE TOPIC `dir2/topic3`");

        // StartFrom is exclusive: equal name is skipped.
        auto resp = GetListing(setup->GetRuntime(), true, Nothing(), "dir2/topic3");
        AssertTopics(resp, {"topic1"});

        resp = GetListing(setup->GetRuntime(), true, Nothing(), "dir1/topic2");
        AssertTopics(resp, {"dir2/topic3", "topic1"});
    }

    Y_UNIT_TEST(CdcStreamsListedWhenRecursive) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"table1/feed"});

        resp = GetListing(setup->GetRuntime(), false);
        AssertTopics(resp, {});
    }

    // -------------------------------------------------------------------------
    // P1 — nesting / filters / combined paging
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(NestedMultiLevel) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        MkDir(*setup, "/Root", "a");
        MkDir(*setup, "/Root/a", "b");
        ExecuteDDL(*setup, "CREATE TOPIC `a/b/topic`");
        ExecuteDDL(*setup, "CREATE TOPIC root_topic");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"a/b/topic", "root_topic"});

        resp = GetListing(setup->GetRuntime(), false);
        AssertTopics(resp, {"root_topic"});
    }

    Y_UNIT_TEST(TableWithoutCdcIgnored) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"topic1"});
    }

    Y_UNIT_TEST(MixTopicsDirsAndCdc) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        MkDir(*setup, "/Root", "dir1");
        ExecuteDDL(*setup, "CREATE TOPIC `dir1/topic2`");
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"dir1/topic2", "table1/feed", "topic1"});
    }

    Y_UNIT_TEST(StartFromLastReturnsEmpty) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic_a");
        ExecuteDDL(*setup, "CREATE TOPIC topic_b");

        auto resp = GetListing(setup->GetRuntime(), true, Nothing(), "topic_b");
        AssertTopics(resp, {});
    }

    Y_UNIT_TEST(StartFromWithLimit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic_a");
        ExecuteDDL(*setup, "CREATE TOPIC topic_b");
        ExecuteDDL(*setup, "CREATE TOPIC topic_c");
        ExecuteDDL(*setup, "CREATE TOPIC topic_d");

        auto resp = GetListing(setup->GetRuntime(), true, 2, "topic_a");
        AssertTopics(resp, {"topic_b", "topic_c"}, true);

        resp = GetListing(setup->GetRuntime(), true, 2, "topic_c");
        AssertTopics(resp, {"topic_d"}, false);
    }

    Y_UNIT_TEST(FullPagingWalk) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic_a");
        ExecuteDDL(*setup, "CREATE TOPIC topic_b");
        ExecuteDDL(*setup, "CREATE TOPIC topic_c");

        TVector<TString> collected;
        TString startFrom;
        for (ui32 i = 0; i < 5; ++i) {
            auto resp = GetListing(setup->GetRuntime(), true, 1, startFrom);
            UNIT_ASSERT_VALUES_EQUAL(resp->Topics.size(), 1u);
            collected.push_back(resp->Topics[0]);
            startFrom = resp->Topics[0];
            if (!resp->HaveMoreTopics) {
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(collected, (TVector<TString>{"topic_a", "topic_b", "topic_c"}));
    }

    Y_UNIT_TEST(ResponseStatusSuccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto resp = GetListing(setup->GetRuntime(), true);
        UNIT_ASSERT_VALUES_EQUAL(resp->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(resp->Error.empty());
        UNIT_ASSERT(!resp->HaveMoreTopics);
    }

    // -------------------------------------------------------------------------
    // P2 — edges
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(DeepNesting) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        MkDir(*setup, "/Root", "d1");
        MkDir(*setup, "/Root/d1", "d2");
        MkDir(*setup, "/Root/d1/d2", "d3");
        ExecuteDDL(*setup, "CREATE TOPIC `d1/d2/d3/deep`");
        ExecuteDDL(*setup, "CREATE TOPIC shallow");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"d1/d2/d3/deep", "shallow"});
    }

    Y_UNIT_TEST(LimitZeroWithTopics) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto resp = GetListing(setup->GetRuntime(), true, 0);
        AssertTopics(resp, {}, true);
    }

    Y_UNIT_TEST(LimitZeroEmptyRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        auto resp = GetListing(setup->GetRuntime(), true, 0);
        AssertTopics(resp, {}, false);
    }

    Y_UNIT_TEST(ManySiblingTopics) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        TVector<TString> expected;
        for (ui32 i = 0; i < 20; ++i) {
            TString name = TStringBuilder() << "topic_" << Sprintf("%02u", i);
            ExecuteDDL(*setup, TStringBuilder() << "CREATE TOPIC `" << name << "`");
            expected.push_back(name);
        }
        Sort(expected.begin(), expected.end());

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, expected);

        resp = GetListing(setup->GetRuntime(), true, 5);
        AssertTopics(resp, TVector<TString>(expected.begin(), expected.begin() + 5), true);
    }

    Y_UNIT_TEST(TopicAndCdcTogether) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC plain");
        ExecuteDDL(*setup, "CREATE TABLE t (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE t ADD CHANGEFEED cdc WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto resp = GetListing(setup->GetRuntime(), true);
        AssertTopics(resp, {"plain", "t/cdc"});
    }

    Y_UNIT_TEST(EmptyTokenAccepted) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        EnableLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto resp = GetListing(setup->GetRuntime(), true, {}, {}, "/Root", "");
        AssertTopics(resp, {"topic1"});
    }

}

} // namespace NKikimr::NPQ
