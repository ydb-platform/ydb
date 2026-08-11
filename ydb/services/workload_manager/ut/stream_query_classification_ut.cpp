#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

namespace NKikimr::NWorkloadManager {

using namespace NWorkloadManager;
using namespace NYdb;


namespace {

TIntrusivePtr<IYdbSetup> MakeStreamingYdb() {
    return TYdbSetupSettings()
        .EnableHasPredicatesInResourcePoolClassifiers(true)
        .Create([](auto) {});
}

void CreateTopic(TIntrusivePtr<IYdbSetup> ydb, TString name) {
    const auto& result = ydb->ExecuteQuery(TStringBuilder() << "CREATE TOPIC " << name);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(StreamingQueryClassification) {
    void CreateStreamingPoolAndClassifier(TIntrusivePtr<IYdbSetup> ydb, const TString& poolId, const TString& classifierSql) {
        const auto& result = ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT = 10,
                QUEUE_SIZE = 100,
                TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 10,
                QUERY_CPU_LIMIT_PERCENT_PER_NODE = 1
            );
            )" << classifierSql);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        ydb->WaitForClassifierPropagation();
    }

    void CreateAndWaitStreamingQuery(TIntrusivePtr<IYdbSetup> ydb, const TString& createSql, const TString& poolId) {
        const auto& result = ydb->ExecuteQuery(createSql);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        // Wait until RunningRequests stays at 1 for a short stable window (avoid fixed sleep).
        const TPoolStateDescription expected{.DelayedRequests = 0, .RunningRequests = 1};
        const TDuration stableFor = TDuration::Seconds(1);
        TInstant stableSince;
        IYdbSetup::WaitFor(FUTURE_WAIT_TIMEOUT, "stable streaming pool state", [&](TString& errorString) {
            const auto description = ydb->GetPoolDescription(TDuration::Zero(), poolId);
            errorString = TStringBuilder()
                << "delayed = " << description.DelayedRequests
                << ", running = " << description.RunningRequests;
            if (description.DelayedRequests == expected.DelayedRequests
                    && description.RunningRequests == expected.RunningRequests) {
                if (!stableSince) {
                    stableSince = TInstant::Now();
                }
                return TInstant::Now() - stableSince >= stableFor;
            }
            stableSince = {};
            return false;
        });
    }

    Y_UNIT_TEST(TestStreamingQueryClassificationByPath) {
        auto ydb = MakeStreamingYdb();

        const TString poolId = "streaming_pool";
        CreateTopic(ydb, "input_topic");
        CreateTopic(ydb, "output_topic");

        CreateStreamingPoolAndClassifier(ydb, poolId, TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER streaming_classifier WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_PATH = "*input_topic*"
            );
        )");

        CreateAndWaitStreamingQuery(ydb, R"(
            CREATE STREAMING QUERY MyStreamingQuery
            AS DO BEGIN
                INSERT INTO output_topic SELECT * FROM input_topic;
            END DO
        )", poolId);
    }

    Y_UNIT_TEST(TestClassifierMatchesStreamingQuery) {
        auto ydb = MakeStreamingYdb();

        const TString poolId = "streaming_pool";
        CreateTopic(ydb, "input_topic");
        CreateTopic(ydb, "output_topic");

        CreateStreamingPoolAndClassifier(ydb, poolId, TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER streaming_classifier WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_STREAM = "true"
            );
        )");

        CreateAndWaitStreamingQuery(ydb, R"(
            CREATE STREAMING QUERY MyStreamingQuery
            AS DO BEGIN
                INSERT INTO output_topic SELECT * FROM input_topic;
            END DO
        )", poolId);
    }

    Y_UNIT_TEST(TestStreamingQueryUsesExplicitResourcePool) {
        auto ydb = MakeStreamingYdb();

        const TString classifierPoolId = "classifier_pool";
        const TString explicitPoolId = "explicit_pool";
        CreateTopic(ydb, "input_topic");
        CreateTopic(ydb, "output_topic");

        {
            const auto& result = ydb->ExecuteQuery(TStringBuilder() << R"(
                CREATE RESOURCE POOL )" << classifierPoolId << R"( WITH (
                    CONCURRENT_QUERY_LIMIT = 10,
                    QUEUE_SIZE = 100,
                    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 10,
                    QUERY_CPU_LIMIT_PERCENT_PER_NODE = 1
                );
                CREATE RESOURCE POOL )" << explicitPoolId << R"( WITH (
                    CONCURRENT_QUERY_LIMIT = 10,
                    QUEUE_SIZE = 100,
                    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 10,
                    QUERY_CPU_LIMIT_PERCENT_PER_NODE = 1
                );
                CREATE RESOURCE POOL CLASSIFIER streaming_classifier WITH (
                    RESOURCE_POOL=")" << classifierPoolId << R"(",
                    HAS_STREAM = "true"
                );
            )");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
        ydb->WaitForClassifierPropagation();

        CreateAndWaitStreamingQuery(ydb, TStringBuilder() << R"(
            CREATE STREAMING QUERY MyStreamingQuery WITH (
                RESOURCE_POOL = ")" << explicitPoolId << R"("
            ) AS DO BEGIN
                INSERT INTO output_topic SELECT * FROM input_topic;
            END DO
        )", explicitPoolId);

        ydb->WaitPoolState({.DelayedRequests = 0, .RunningRequests = 0}, classifierPoolId);
    }
}

namespace {

using namespace fmt::literals;

void CheckObjectProperties(TTestActorRuntime& runtime, const TString& path, const std::unordered_map<TString, TString>& expectedProperties) {
    auto streamingQueryDesc = NKqp::Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& streamingQuery = streamingQueryDesc->ResultSet.at(0);
    UNIT_ASSERT_VALUES_EQUAL(streamingQuery.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindStreamingQuery);
    UNIT_ASSERT(streamingQuery.StreamingQueryInfo);
    UNIT_ASSERT_VALUES_EQUAL(streamingQuery.StreamingQueryInfo->Description.GetName(), SplitPath(path).back());
    const auto& properties = streamingQuery.StreamingQueryInfo->Description.GetProperties().GetProperties();
    UNIT_ASSERT_GE(properties.size(), expectedProperties.size());
    for (const auto& [key, value] : expectedProperties) {
        UNIT_ASSERT_C(properties.contains(key), key);
        UNIT_ASSERT_VALUES_EQUAL(properties.at(key), value);
    }
}

std::unique_ptr<NKqp::TKikimrRunner> SetupStreamingSource(bool enableStreamingQueries = true) {
    NKikimrConfig::TAppConfig config;
    auto& featureFlags = *config.MutableFeatureFlags();
    featureFlags.SetEnableStreamingQueries(enableStreamingQueries);
    featureFlags.SetEnableExternalDataSources(true);
    featureFlags.SetEnableResourcePools(true);
    featureFlags.SetEnableStreamingQueryDisposition(true);
    config.MutableTableServiceConfig()->SetDqChannelVersion(1u);

    auto kikimr = std::make_unique<NKqp::TKikimrRunner>(NKqp::TKikimrSettings(config)
        .SetEnableStreamingQueries(enableStreamingQueries)
        .SetEnableExternalDataSources(true)
        .SetEnableResourcePools(true)
        .SetInitFederatedQuerySetupFactory(true));

    const auto result = kikimr->GetQueryClient().ExecuteQuery(fmt::format(R"(
        CREATE TOPIC MyTopic;
        CREATE EXTERNAL DATA SOURCE MySource WITH (
            SOURCE_TYPE = "Ydb",
            LOCATION = "localhost:{port}",
            DATABASE_NAME = "/Root",
            AUTH_METHOD = "NONE"
        );)",
        "port"_a = kikimr->GetTestServer().GetGRpcServer().GetPort()),
        NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());

    return kikimr;
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(WorkloadManagerScheme) {
    Y_UNIT_TEST(StreamingQueriesWithResourcePools) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = kikimr->GetQueryClient().ExecuteQuery(R"(
                CREATE RESOURCE POOL my_pool WITH (
                    CONCURRENT_QUERY_LIMIT = 0
                ))",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = TRUE,
                    RESOURCE_POOL = "my_pool"
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Resource pool my_pool was disabled due to zero concurrent query limit");

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {});
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/OtherQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/OtherQuery", {});
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/OtherQuery` SET (
                    RUN = TRUE,
                    RESOURCE_POOL = "my_pool"
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Resource pool my_pool was disabled due to zero concurrent query limit");
        }
    }
}

}  // namespace NKikimr::NWorkloadManager
