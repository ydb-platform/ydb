#include "common.h"

#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>


namespace NKikimr::NKqp::NFederatedQueryTest {

using namespace fmt::literals;

namespace {

// String emitted by the workload service when a query routes to a pool whose
// concurrent_query_limit is 0. Kept as a constant so tests read cleanly.
constexpr TStringBuf REJECT_ERROR = "Resource pool reject was disabled due to zero concurrent query limit";

TString RejectClassifierDdl(TStringBuf classifierName, TStringBuf hasPath) {
    return TStringBuilder() << R"(
        CREATE RESOURCE POOL reject WITH (concurrent_query_limit = 0);
        CREATE RESOURCE POOL CLASSIFIER )" << classifierName << R"( WITH (
            resource_pool = 'reject',
            has_path = ')" << hasPath << R"('
        );
    )";
}

}  // anonymous namespace


// HAS_PATH end-to-end coverage for the object kinds whose fixture requirements
// (real local PQ, mock connector, mock PQ gateway, http gateway) are only met
// by TStreamingTestFixture. Cheaper kinds (regular tables, sysview, secondary
// index, view underlying) live in ydb/services/workload_manager/ut.
Y_UNIT_TEST_SUITE(HasPathDatastreams) {

    // KindTopic — direct read of a local topic (no EDS in the chain).
    // The topic path lands in tx.Tables (B walk).
    Y_UNIT_TEST_F(DirectTopicMatches, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
        featureFlags.SetEnableTopicsSqlIoOperations(true);
        featureFlags.SetEnableHasPredicatesInResourcePoolClassifiers(true);

        constexpr TStringBuf topicName = "test_topic";
        CreateTopic(std::string(topicName), std::nullopt, /*local*/ true);

        ExecSchemeQuery(RejectClassifierDdl(
            "hp_direct_topic", "/Root/test_topic"));

        WaitForClassifierPropagation();

        ExecQuery(R"(
            SELECT * FROM `/Root/test_topic` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (key String NOT NULL, value String NOT NULL)
            ) LIMIT 1;
        )", NYdb::EStatus::PRECONDITION_FAILED, std::string(REJECT_ERROR));
    }

    // KindCdcStream — changefeed on a local table. Same proto shape as a
    // direct topic; path is `<table>/<stream>`.
    Y_UNIT_TEST_F(CdcStreamMatches, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
        featureFlags.SetEnableTopicsSqlIoOperations(true);
        featureFlags.SetEnableHasPredicatesInResourcePoolClassifiers(true);

        ExecSchemeQuery(RejectClassifierDdl(
            "hp_cdc", "/Root/t_cdc/cf"));

        WaitForClassifierPropagation();

        ExecSchemeQuery(R"(
            CREATE TABLE t_cdc (
                Id Uint64 NOT NULL,
                Payload Utf8,
                PRIMARY KEY (Id)
            );
            ALTER TABLE t_cdc ADD CHANGEFEED cf WITH (
                MODE = 'UPDATES',
                FORMAT = 'JSON'
            );
        )");


        ExecQuery(R"(
            SELECT * FROM `/Root/t_cdc/cf` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (key String NOT NULL, value String NOT NULL)
            ) LIMIT 1;
        )", NYdb::EStatus::PRECONDITION_FAILED, std::string(REJECT_ERROR));
    }

    // KindExternalDataSource — gate by the local EDS path.
    // Uses a loopback Ydb-typed EDS pointing at our own runner + a local topic
    // as the target object (SetupMockPqGateway + real local PQ serves reads).
    Y_UNIT_TEST_F(FederatedEdsPathMatches, TStreamingTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableHasPredicatesInResourcePoolClassifiers(true);
        SetupMockPqGateway();
        constexpr TStringBuf topicName = "eds_topic";
        CreateTopic(std::string(topicName), std::nullopt, /*local*/ true);

        ExecSchemeQuery(RejectClassifierDdl(
            "hp_eds_local", "/Root/eds"));

        WaitForClassifierPropagation();

        ExecSchemeQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE eds WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint}",
                DATABASE_NAME = "/Root",
                AUTH_METHOD = "NONE"
            );
        )", "endpoint"_a = GetKikimrRunner()->GetEndpoint()));


        ExecQuery(fmt::format(R"(
            SELECT * FROM eds.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (key String NOT NULL, value String NOT NULL)
            ) LIMIT 1;
        )", "topic"_a = topicName),
            NYdb::EStatus::PRECONDITION_FAILED, std::string(REJECT_ERROR));
    }

    // KindTopic — via EDS. Gate by the remote topic name (bare name that only
    // appears in TDqPqTopicSource.TopicPath — the (D) walk's unique-coverage
    // case verified on real cluster).
    Y_UNIT_TEST_F(FederatedTopicRemoteNameMatches, TStreamingTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableHasPredicatesInResourcePoolClassifiers(true);
        SetupMockPqGateway();
        constexpr TStringBuf topicName = "remote_by_name_topic";
        CreateTopic(std::string(topicName), std::nullopt, /*local*/ true);

        // CanonizePath prepends a leading slash to the bare TopicPath emitted
        // by TDqPqTopicSource, so the regex is anchored on `/<topic>`.
        ExecSchemeQuery(RejectClassifierDdl(
            "hp_remote_name", "/remote_by_name_topic"));

        WaitForClassifierPropagation();

        ExecSchemeQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE eds WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint}",
                DATABASE_NAME = "/Root",
                AUTH_METHOD = "NONE"
            );
        )", "endpoint"_a = GetKikimrRunner()->GetEndpoint()));


        ExecQuery(fmt::format(R"(
            SELECT * FROM eds.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (key String NOT NULL, value String NOT NULL)
            ) LIMIT 1;
        )", "topic"_a = topicName),
            NYdb::EStatus::PRECONDITION_FAILED, std::string(REJECT_ERROR));
    }

    // KindExternalTable — External Table on top of an ObjectStorage EDS.
    // Both the ET path and the underlying EDS path land in tx.Tables (B walk).
    Y_UNIT_TEST_F(ExternalTableMatches, TStreamingTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableHasPredicatesInResourcePoolClassifiers(true);

        ExecSchemeQuery(RejectClassifierDdl(
            "hp_et", "/Root/et_s3"));

        WaitForClassifierPropagation();

        ExecSchemeQuery(R"(
            CREATE EXTERNAL DATA SOURCE eds_s3 WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "https://storage.yandexcloud.net/tpc/",
                AUTH_METHOD = "NONE"
            );
            CREATE EXTERNAL TABLE et_s3 (
                o_orderkey Int32,
                o_orderstatus String
            ) WITH (
                DATA_SOURCE = "eds_s3",
                LOCATION = "h/s1/parquet/orders/",
                FORMAT = "parquet",
                FILE_PATTERN = "*.parquet"
            );
        )");

        ExecQuery(R"(
            SELECT * FROM et_s3 LIMIT 1;
        )", NYdb::EStatus::PRECONDITION_FAILED, std::string(REJECT_ERROR));
    }
}

}  // namespace NKikimr::NKqp::NFederatedQueryTest
