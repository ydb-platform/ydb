#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpSchemeExternalTable) {
    Y_UNIT_TEST(CreateExternalTable) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        config.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        config.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(/* value */ true);
        TKikimrRunner kikimr{ NKqp::TKikimrSettings(config) };

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        {
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
            UNIT_ASSERT(externalTable.ExternalTableInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetSourceType(), "ObjectStorage");
        }

        auto queryClient = kikimr.GetQueryClient();
        {
            auto query = TStringBuilder() << R"(
                CREATE OR REPLACE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/other/location/"
                );)";
            auto result = queryClient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/other/location/");
        }

        {
            const auto result = queryClient.ExecuteQuery(fmt::format(R"(
                CREATE OR REPLACE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="YT",
                    LOCATION="other-bucket",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName
            ), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Changing external data source type is not allowed");
        }
    }

    Y_UNIT_TEST(DisableCreateExternalTable) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ false);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="/Root/ExternalDataSource",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External tables are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DisableCreateExternalTableByAvailableFlag) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ true);
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="/Root/ExternalDataSource",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalDataSource', error: path hasn't been resolved, nearest resolved path: '/Root'", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalTableCheckPrimaryKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY(Key)
            ) WITH (
                DATA_SOURCE="/Root/MyDataSource",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "PRIMARY KEY is not supported for external table", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalTableValidation) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY(Key)
            ) WITH (
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "DATA_SOURCE requires key", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DropExternalTable) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        config.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr{ NKqp::TKikimrSettings(config) };

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL TABLE `)" << externalTableName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalTableDesc->ErrorCount, 1);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalDataSourceDesc->ErrorCount, 1);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
        }
    }

    Y_UNIT_TEST(DisableDropExternalTable) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ false);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"( DROP EXTERNAL TABLE `/Root/ExternalDataSource`)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External table are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalTableWithSettings) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        config.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr{ NKqp::TKikimrSettings(config) };

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String,
                year Int64 NOT NULL,
                month Int64 NOT NULL
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/folder1/",
                FORMAT="json_as_string",
                `projection.enabled`="true",
                `projection.year.type`="integer",
                `projection.year.min`="2010",
                `projection.year.max`="2022",
                `projection.year.interval`="1",
                `projection.month.type`="integer",
                `projection.month.min`="1",
                `projection.month.max`="12",
                `projection.month.interval`="1",
                `projection.month.digits`="2",
                `storage.location.template`="${year}/${month}",
                PARTITIONED_BY = "[year, month]"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
        UNIT_ASSERT(externalTable.ExternalTableInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/folder1/");
    }

    Y_UNIT_TEST(CreateExternalTableWithUpperCaseSettings) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        settings.AppConfig.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr(settings);

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String,
                Year Int64 NOT NULL,
                Month Int64 NOT NULL
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/folder1/",
                FORMAT="json_as_string",
                `projection.enabled`="true",
                `projection.Year.type`="integer",
                `projection.Year.min`="2010",
                `projection.Year.max`="2022",
                `projection.Year.interval`="1",
                `projection.Month.type`="integer",
                `projection.Month.min`="1",
                `projection.Month.max`="12",
                `projection.Month.interval`="1",
                `projection.Month.digits`="2",
                `storage.location.template`="${Year}/${Month}",
                PARTITIONED_BY = "[Year, Month]"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
        UNIT_ASSERT(externalTable.ExternalTableInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/folder1/");
    }

    Y_UNIT_TEST(DoubleCreateExternalTable) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        config.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr{ NKqp::TKikimrSettings(config) };

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
            UNIT_ASSERT(externalTable.ExternalTableInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetSourceType(), "ObjectStorage");
        }

        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalTable', error: path exist", result.GetIssues().ToString());
        }
    }
}

} // namespace NKikimr::NKqp
