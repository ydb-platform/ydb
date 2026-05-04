#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/util/aws.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/split.h>

#include <array>
#include <cstdio>
#include <fmt/format.h>
#include <memory>
#include <stdexcept>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace fmt::literals;

namespace {

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }

    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }

    return result;
}

TString GetExternalPort(const TString& service, const TString& port) {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "docker-compose.yml");
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port " + service + " " + port)).Split(':').ToList<TString>();
    return result ? Strip(result.back()) : TString{};
}

std::unique_ptr<TKikimrRunner> MakeS3TieringKikimrRunner(const bool enableReplaceIfExistsForExternalEntities, const bool grantConnectRoot1) {
    auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();

    TKikimrSettings runnerSettings;
    runnerSettings.WithSampleTables = false;
    runnerSettings.SetColumnShardAlterObjectEnabled(true);
    runnerSettings.SetS3ActorsFactory(s3ActorsFactory);

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableColumnshardBool(true);
    featureFlags.SetEnableColumnStore(true);
    featureFlags.SetEnableTieringInColumnShard(true);
    featureFlags.SetEnableExternalDataSources(true);

    if (enableReplaceIfExistsForExternalEntities) {
        featureFlags.SetEnableReplaceIfExistsForExternalEntities(true);
    }

    runnerSettings.SetFeatureFlags(featureFlags);
    runnerSettings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");

    auto kikimr = std::make_unique<TKikimrRunner>(runnerSettings);

    if (grantConnectRoot1) {
        kikimr->GetTestClient().GrantConnect("root1@builtin");
    }

    return kikimr;
}

void CreateTestOlapTableForTiering(TKikimrRunner& kikimr) {
    TLocalHelper olapHelper(kikimr);
    olapHelper.CreateTestOlapTable("olapTable", "olapStore", 4, 4);
}

void ConfigureOlapStoreTieringCompaction(NYdb::NTable::TSession& session, const TString& storePath) {
    const TString query = fmt::format(R"(
        ALTER OBJECT `{store_path}` (TYPE TABLESTORE) SET (
            ACTION=UPSERT_OPTIONS,
            `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
            `COMPACTION_PLANNER.FEATURES`=`{features}`
        );
    )",
        "store_path"_a = storePath,
        "features"_a =
            R"({"levels":[{"class_name":"Zero","portions_live_duration":"5s","expected_blobs_size":1000000000000,"portions_count_available":2},{"class_name":"Zero"}]})"
    );

    auto result = session.ExecuteSchemeQuery(query).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
}

void FillTieringOlapTableWithTestData(TKikimrRunner& kikimr, const TString& tablePath, const ui64 fromIndex, const ui64 toIndex) {
    for (ui64 i = fromIndex; i < toIndex; ++i) {
        WriteTestData(kikimr, tablePath, 0, 3600000000 + i * 10000, 1000);
        WriteTestData(kikimr, tablePath, 0, 3600000000 + i * 10000, 1000);
    }
}

void CreateTierExternalDataSource(
    NYdb::NTable::TSession& session,
    const TString& tierPath,
    const TString& location,
    const TString& accessKeySecretName,
    const TString& secretKeySecretName,
    const bool replace) {
    const TString query = fmt::format(R"(
        {create_stmt} EXTERNAL DATA SOURCE `{tier_path}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="{location}",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_secret}",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{secret_key_secret}",
            AWS_REGION="ru-central-1"
        );
    )",
        "create_stmt"_a = replace ? "CREATE OR REPLACE" : "CREATE",
        "tier_path"_a = tierPath,
        "location"_a = location,
        "access_key_secret"_a = accessKeySecretName,
        "secret_key_secret"_a = secretKeySecretName
    );

    auto result = session.ExecuteSchemeQuery(query).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
}

} // namespace

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

Y_UNIT_TEST_SUITE(OlapTieringS3Secrets) {
    Y_UNIT_TEST(TieringSecretAccessCheck) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const TString tierPath = "/Root/tier1";
        const TString accessKeySecretName = "/Root/access-key-secret";
        const TString secretKeySecretName = "/Root/secret-key-secret";
        const TString columnName = "timestamp";

        auto kikimr = MakeS3TieringKikimrRunner(false, true);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        CreateTestOlapTableForTiering(*kikimr);
        const TString storePath = "/Root/olapStore";
        ConfigureOlapStoreTieringCompaction(session, storePath);

        {
            const TString query = fmt::format(R"(
                CREATE SECRET `{access_key_secret}` WITH (value = "minio");
                CREATE SECRET `{secret_key_secret}` WITH (value = "minio123");
            )",
                "access_key_secret"_a = accessKeySecretName,
                "secret_key_secret"_a = secretKeySecretName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(
            session,
            tierPath,
            "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/",
            accessKeySecretName,
            secretKeySecretName,
            false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 0, 30);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = kikimr->GetTableClient();
        TString selectQuery = fmt::format(R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `{table_path}/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )",
            "table_path"_a = tablePath
        );

        const TDuration evictionWaitTimeout = TDuration::Seconds(50);
        const TDuration evictionPollInterval = TDuration::Seconds(10);
        TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
            UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                fmt::format("Eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                    evictionWaitTimeout.Seconds(), rows.size(), tierPath));
            Sleep(evictionPollInterval);
            rows = ExecuteScanQuery(tableClient, selectQuery);
        }

        TString tierName = GetUtf8(rows[0].at("TierName"));
        UNIT_ASSERT_VALUES_EQUAL(tierName, tierPath);
    }

    Y_UNIT_TEST(TieringSecretMigration) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const TString tierPath = "/Root/tier1";
        const TString oldAccessKeyName = "accessKey";
        const TString oldSecretKeyName = "secretKey";
        const TString newAccessKeySecretName = "/Root/new-access-key-secret";
        const TString newSecretKeySecretName = "/Root/new-secret-key-secret";
        const TString columnName = "timestamp";

        auto kikimr = MakeS3TieringKikimrRunner(true, false);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        CreateTestOlapTableForTiering(*kikimr);
        const TString storePath = "/Root/olapStore";
        ConfigureOlapStoreTieringCompaction(session, storePath);

        {
            const TString query = fmt::format(R"(
                CREATE OBJECT {access_key} (TYPE SECRET) WITH (value = "minio");
                CREATE OBJECT {secret_key} (TYPE SECRET) WITH (value = "minio123");
            )",
                "access_key"_a = oldAccessKeyName,
                "secret_key"_a = oldSecretKeyName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(
            session,
            tierPath,
            "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/",
            oldAccessKeyName,
            oldSecretKeyName,
            false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 0, 30);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = kikimr->GetTableClient();
        TString selectQuery = fmt::format(R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `{table_path}/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )",
            "table_path"_a = tablePath
        );

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("Old secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
        }

        {
            const TString query = fmt::format(R"(
                CREATE SECRET `{access_key_secret}` WITH (value = "minio");
                CREATE SECRET `{secret_key_secret}` WITH (value = "minio123");
            )",
                "access_key_secret"_a = newAccessKeySecretName,
                "secret_key_secret"_a = newSecretKeySecretName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(
            session,
            tierPath,
            "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/",
            newAccessKeySecretName,
            newSecretKeySecretName,
            true /*replace*/);

        csController->WaitActualization(TDuration::Seconds(5));

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 30, 60);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("New secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
        }
    }

    Y_UNIT_TEST(TieringInvalidSecrets) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const TString tierPath = "/Root/tier1";
        const TString invalidAccessKeyName = "badAccessKey";
        const TString invalidSecretKeyName = "badSecretKey";
        const TString validAccessKeySecretName = "/Root/valid-access-key-secret";
        const TString validSecretKeySecretName = "/Root/valid-secret-key-secret";
        const TString columnName = "timestamp";

        auto kikimr = MakeS3TieringKikimrRunner(true, false);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        CreateTestOlapTableForTiering(*kikimr);
        const TString storePath = "/Root/olapStore";
        ConfigureOlapStoreTieringCompaction(session, storePath);

        {
            const TString query = fmt::format(R"(
                CREATE OBJECT {access_key} (TYPE SECRET) WITH (value = "INVALID_KEY");
                CREATE OBJECT {secret_key} (TYPE SECRET) WITH (value = "INVALID_SECRET");
            )",
                "access_key"_a = invalidAccessKeyName,
                "secret_key"_a = invalidSecretKeyName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(
            session,
            tierPath,
            "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/",
            invalidAccessKeyName,
            invalidSecretKeyName,
            false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 0, 30);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = kikimr->GetTableClient();
        TString selectQuery = fmt::format(R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `{table_path}/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )",
            "table_path"_a = tablePath
        );

        {
            const TDuration noEvictionCheckDuration = TDuration::Seconds(30);
            const TDuration pollInterval = TDuration::Seconds(10);
            TInstant checkDeadline = TInstant::Now() + noEvictionCheckDuration;
            while (TInstant::Now() < checkDeadline) {
                auto rows = ExecuteScanQuery(tableClient, selectQuery);
                for (auto&& row : rows) {
                    UNIT_ASSERT_C(GetUtf8(row.at("TierName")) != tierPath,
                        "Data was unexpectedly evicted to tier with invalid secrets");
                }

                Sleep(pollInterval);
            }
        }

        {
            const TString query = fmt::format(R"(
                CREATE SECRET `{access_key_secret}` WITH (value = "minio");
                CREATE SECRET `{secret_key_secret}` WITH (value = "minio123");
            )",
                "access_key_secret"_a = validAccessKeySecretName,
                "secret_key_secret"_a = validSecretKeySecretName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(
            session,
            tierPath,
            "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/",
            validAccessKeySecretName,
            validSecretKeySecretName,
            true /*replace*/);

        csController->WaitActualization(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("Valid secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
        }
    }

    Y_UNIT_TEST(TieringSecretMigrationViaDropCreate) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const TString tierPath = "/Root/tier1";
        const TString oldAccessKeyName = "accessKey";
        const TString oldSecretKeyName = "secretKey";
        const TString newAccessKeySecretName = "/Root/new-access-key-secret";
        const TString newSecretKeySecretName = "/Root/new-secret-key-secret";
        const TString columnName = "timestamp";

        auto kikimr = MakeS3TieringKikimrRunner(false, false);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        CreateTestOlapTableForTiering(*kikimr);
        const TString storePath = "/Root/olapStore";
        ConfigureOlapStoreTieringCompaction(session, storePath);

        {
            const TString query = fmt::format(R"(
                CREATE OBJECT {access_key} (TYPE SECRET) WITH (value = "minio");
                CREATE OBJECT {secret_key} (TYPE SECRET) WITH (value = "minio123");
            )",
                "access_key"_a = oldAccessKeyName,
                "secret_key"_a = oldSecretKeyName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString minioLocation = "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/";

        CreateTierExternalDataSource(session, tierPath, minioLocation, oldAccessKeyName, oldSecretKeyName, false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 0, 30);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = kikimr->GetTableClient();
        TString selectQuery = fmt::format(R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `{table_path}/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )",
            "table_path"_a = tablePath
        );

        ui64 rowsBeforeMigration = 0;

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("Old secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
            rowsBeforeMigration = GetUint64(rows[0].at("Rows"));
            UNIT_ASSERT_C(rowsBeforeMigration > 0, "Expected non-zero row count before migration");
        }

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` RESET (TTL);
            )",
                "table_path"_a = tablePath
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = fmt::format(R"(
                DROP EXTERNAL DATA SOURCE `{tier_path}`;
            )",
                "tier_path"_a = tierPath
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = fmt::format(R"(
                CREATE SECRET `{access_key_secret}` WITH (value = "minio");
                CREATE SECRET `{secret_key_secret}` WITH (value = "minio123");
            )",
                "access_key_secret"_a = newAccessKeySecretName,
                "secret_key_secret"_a = newSecretKeySecretName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(session, tierPath, minioLocation, newAccessKeySecretName, newSecretKeySecretName, false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        csController->WaitActualization(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("New secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
            ui64 rowsAfterMigration = GetUint64(rows[0].at("Rows"));
            UNIT_ASSERT_VALUES_EQUAL_C(rowsAfterMigration, rowsBeforeMigration,
                fmt::format("Row count changed after migration: before={}, after={}", rowsBeforeMigration, rowsAfterMigration));
        }
    }

    Y_UNIT_TEST(TieringInvalidSecretsFixViaDropCreateCheck) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const TString tierPath = "/Root/tier1";
        const TString invalidAccessKeyName = "badAccessKey";
        const TString invalidSecretKeyName = "badSecretKey";
        const TString validAccessKeySecretName = "/Root/valid-access-key-secret";
        const TString validSecretKeySecretName = "/Root/valid-secret-key-secret";
        const TString columnName = "timestamp";

        auto kikimr = MakeS3TieringKikimrRunner(false, false);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        CreateTestOlapTableForTiering(*kikimr);
        const TString storePath = "/Root/olapStore";
        ConfigureOlapStoreTieringCompaction(session, storePath);

        {
            const TString query = fmt::format(R"(
                CREATE OBJECT {access_key} (TYPE SECRET) WITH (value = "INVALID_KEY");
                CREATE OBJECT {secret_key} (TYPE SECRET) WITH (value = "INVALID_SECRET");
            )",
                "access_key"_a = invalidAccessKeyName,
                "secret_key"_a = invalidSecretKeyName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString minioLocation = "http://localhost:" + GetExternalPort("minio", "9000") + "/datalake/";

        CreateTierExternalDataSource(session, tierPath, minioLocation, invalidAccessKeyName, invalidSecretKeyName, false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        FillTieringOlapTableWithTestData(*kikimr, tablePath, 0, 30);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = kikimr->GetTableClient();
        TString selectQuery = fmt::format(R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `{table_path}/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )",
            "table_path"_a = tablePath
        );

        {
            const TDuration noEvictionCheckDuration = TDuration::Seconds(30);
            const TDuration pollInterval = TDuration::Seconds(10);
            TInstant checkDeadline = TInstant::Now() + noEvictionCheckDuration;
            while (TInstant::Now() < checkDeadline) {
                auto rows = ExecuteScanQuery(tableClient, selectQuery);
                for (auto&& row : rows) {
                    UNIT_ASSERT_C(GetUtf8(row.at("TierName")) != tierPath,
                        "Data was unexpectedly evicted to tier with invalid secrets");
                }

                Sleep(pollInterval);
            }
        }

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` RESET (TTL);
            )",
                "table_path"_a = tablePath
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = fmt::format(R"(
                DROP EXTERNAL DATA SOURCE `{tier_path}`;
            )",
                "tier_path"_a = tierPath
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = fmt::format(R"(
                CREATE SECRET `{access_key_secret}` WITH (value = "minio");
                CREATE SECRET `{secret_key_secret}` WITH (value = "minio123");
            )",
                "access_key_secret"_a = validAccessKeySecretName,
                "secret_key_secret"_a = validSecretKeySecretName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        CreateTierExternalDataSource(session, tierPath, minioLocation, validAccessKeySecretName, validSecretKeySecretName, false /*replace*/);

        {
            const TString query = fmt::format(R"(
                ALTER TABLE `{table_path}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{tier_path}` ON `{column_name}`
            )",
                "table_path"_a = tablePath,
                "tier_path"_a = tierPath,
                "column_name"_a = columnName
            );

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        csController->WaitActualization(TDuration::Seconds(5));

        {
            const TDuration evictionWaitTimeout = TDuration::Seconds(50);
            const TDuration evictionPollInterval = TDuration::Seconds(10);
            TInstant evictionDeadline = TInstant::Now() + evictionWaitTimeout;
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            while (rows.size() != 1 || GetUtf8(rows[0].at("TierName")) != tierPath) {
                UNIT_ASSERT_C(TInstant::Now() < evictionDeadline,
                    fmt::format("Valid secrets: eviction didn't complete within {}s: got {} tier(s), expected all data in {}",
                        evictionWaitTimeout.Seconds(), rows.size(), tierPath));
                Sleep(evictionPollInterval);
                rows = ExecuteScanQuery(tableClient, selectQuery);
            }

            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierPath);
        }
    }
}

} // namespace NKikimr::NKqp
