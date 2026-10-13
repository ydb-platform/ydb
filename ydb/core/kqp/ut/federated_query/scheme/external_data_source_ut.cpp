#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

#include <util/string/printf.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace fmt::literals;

namespace {

template<bool UseSchemaSecrets>
void CreateSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
    TString query;

    if constexpr (UseSchemaSecrets) {
        query = Sprintf("CREATE SECRET `%s` WITH (value=\"%s\")", secretName.c_str(), secretValue.c_str());
    } else {
        query = Sprintf("CREATE OBJECT %s (TYPE SECRET) WITH value=\"%s\"", secretName.c_str(), secretValue.c_str());
    }

    const auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
    UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, queryResult.GetStatus(), queryResult.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSchemeExternalDataSource) {
    Y_UNIT_TEST(DisableExternalDataSourcesOnServerless) {
        auto ydb = NWorkloadManager::TYdbSetupSettings()
            .CreateSampleTenants(/* value */ true)
            .EnableExternalDataSourcesOnServerless(/* value */ false)
            .Create();

        auto checkDisabled = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled for serverless domains. Please contact your system administrator to enable it", result.GetIssues().ToString());
        };

        auto checkNotFound = [](const auto& result, const TString& path, const TString& error) {
            const auto& issuesString = result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, issuesString);
            UNIT_ASSERT_STRING_CONTAINS_C(issuesString, TStringBuilder() << "Path `" << path << "` does not exist", issuesString);
            UNIT_ASSERT_STRING_CONTAINS_C(issuesString, error, issuesString);
        };

        const auto& createSourceSql = R"(
            CREATE EXTERNAL DATA SOURCE MyExternalDataSource WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";

        const auto& createTableSql = R"(
            CREATE EXTERNAL TABLE MyExternalTable (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="MyExternalDataSource",
                LOCATION="/"
            );)";

        const auto& dropSourceSql = "DROP EXTERNAL DATA SOURCE MyExternalDataSource;";

        const auto& dropTableSql = "DROP EXTERNAL TABLE MyExternalTable;";

        auto settings = NWorkloadManager::TQueryRunnerSettings().PoolId("");

        // Dedicated, enabled
        settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(ydb->GetDedicatedTenantInfo().NodeIdx);
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSourceSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createTableSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropTableSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSourceSql, settings));

        // Shared, enabled
        settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(ydb->GetSharedTenantInfo().NodeIdx);
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSourceSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createTableSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropTableSql, settings));
        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSourceSql, settings));

        // Serverless, disabled
        settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(ydb->GetServerlessTenantInfo().NodeIdx);
        checkDisabled(ydb->ExecuteQuery(createSourceSql, settings));
        checkDisabled(ydb->ExecuteQuery(createTableSql, settings));
        checkNotFound(ydb->ExecuteQuery(dropTableSql, settings), ydb->GetSettings().GetServerlessTenantName() + "/MyExternalTable", "Executing ESchemeOpDropExternalTable");
        checkNotFound(ydb->ExecuteQuery(dropSourceSql, settings), ydb->GetSettings().GetServerlessTenantName() + "/MyExternalDataSource", "Executing operation with object \"EXTERNAL_DATA_SOURCE\"");
    }

    Y_UNIT_TEST(CreateExternalDataSource) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddHostnamePatterns("my-bucket|other-bucket");
        appCfg.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(/* value */ true);

        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        {
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "my-bucket");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(externalDataSourceName).back());
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasNone());
        }

        auto queryClient = kikimr.GetQueryClient();
        {
            auto query = TStringBuilder() << R"(
                CREATE OR REPLACE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="other-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = queryClient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "other-bucket");
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

    Y_UNIT_TEST_TWIN(CreateExternalDataSourceWithSa, UseSchemaSecrets) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        settings.AppConfig.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr{ settings };

        if (UseSchemaSecrets) {
            kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableSchemaSecrets(/* value */ true);
        }

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        const TString secretId = "mysasignature";
        const TString secretValue = "mysasignaturevalue";
        CreateSecret<UseSchemaSecrets>(secretId, secretValue, session);
        auto query = TStringBuilder() << Sprintf(
            R"(
                CREATE EXTERNAL DATA SOURCE `%s` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="mysa",
                    %s="%s"
                );
            )",
            externalDataSourceName.c_str(),
            UseSchemaSecrets ? "SERVICE_ACCOUNT_SECRET_PATH" : "SERVICE_ACCOUNT_SECRET_NAME",
            secretId.c_str()
        );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
        UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "my-bucket");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(externalDataSourceName).back());
        UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasServiceAccount());
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().GetServiceAccount().GetId(), "mysa");
        UNIT_ASSERT_VALUES_EQUAL(
            externalDataSource.ExternalDataSourceInfo->Description.GetAuth().GetServiceAccount().GetSecretName(),
            UseSchemaSecrets ? "/Root/" + secretId : secretId
        );
    }

    Y_UNIT_TEST(DisableCreateExternalDataSource) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ false);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DisableCreateExternalDataSourceByAvailableFlag) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ true);
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External source with type ObjectStorage is disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(DisableS3ExternalDataSource, UseSchemaSecrets) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(/* value */ false);
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("PostgreSQL");
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);

        if (UseSchemaSecrets) {
            kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableSchemaSecrets(/* value */ true);
        }

        const TString secretId = "secretName" + ToString(UseSchemaSecrets);
        const TString secretValue = "MySecretData";
        const auto okQueryTemplate = R"sql(
            CREATE EXTERNAL DATA SOURCE `%s` WITH (
                SOURCE_TYPE="PostgreSQL",
                LOCATION="my-bucket",
                AUTH_METHOD="BASIC",
                LOGIN="admin",
                %s = "%s",
                DATABASE_NAME="cheburashka"
            );
        )sql";
        const auto failQueryTemplate = R"sql(
            CREATE EXTERNAL DATA SOURCE `{}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
        )sql";

        { // with table client
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            TString externalDataSourceName = "/Root/ExternalDataSource2";
            const auto failQuery = Sprintf(failQueryTemplate, externalDataSourceName.c_str());
            auto result = session.ExecuteSchemeQuery(failQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External source with type ObjectStorage is disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());

            CreateSecret<UseSchemaSecrets>(secretId, secretValue, session);

            const auto okQuery = Sprintf(okQueryTemplate, externalDataSourceName.c_str(), UseSchemaSecrets ? "PASSWORD_SECRET_PATH" : "PASSWORD_SECRET_NAME", secretId.c_str());
            result = session.ExecuteSchemeQuery(okQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { // with query client
            auto client = kikimr.GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();
            TString externalDataSourceName = "/Root/ExternalDataSource";
            const auto failQuery = Sprintf(failQueryTemplate, externalDataSourceName.c_str());
            auto result = session.ExecuteQuery(failQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External source with type ObjectStorage is disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());

            const auto okQuery = Sprintf(okQueryTemplate, externalDataSourceName.c_str(), UseSchemaSecrets ? "PASSWORD_SECRET_PATH" : "PASSWORD_SECRET_NAME", secretId.c_str());
            result = session.ExecuteQuery(okQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidationAuthMethod) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="UNKNOWN"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown AUTH_METHOD = UNKNOWN", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidationSourceType) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="UnknownSourceType",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown source type: UnknownSourceType", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidationLocation) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddHostnamePatterns("common-bucket");
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "It is not allowed to access hostname 'my-bucket'", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DropExternalDataSource) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        settings.AppConfig.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr(settings);

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_EQUAL(externalDataSourceDesc->ErrorCount, 1);
        UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
    }

    Y_UNIT_TEST(DisableDropExternalDataSource) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(/* value */ false);
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ false);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DoubleCreateExternalDataSource) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        settings.AppConfig.MutableQueryServiceConfig()->MutableS3()->SetGeneratorPathsLimit(/* value */ 50000);
        TKikimrRunner kikimr(settings);

        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(/* pos */ 0);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "my-bucket");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(externalDataSourceName).back());
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasNone());
        }

        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalDataSource', error: path exist", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropDependentExternalDataSource) {
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
        {
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

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
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Other entities depend on this data source, please remove them at the beginning: /Root/ExternalTable", result.GetIssues().ToString());
        }

        auto queryClient = kikimr.GetQueryClient();
        {
            const auto result = queryClient.ExecuteQuery(fmt::format(R"(
                CREATE OR REPLACE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="other-bucket",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName
            ), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = session.ExecuteSchemeQuery(fmt::format(
                "DROP EXTERNAL DATA SOURCE `{external_source}`",
                "external_source"_a = externalDataSourceName
            )).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Other entities depend on this data source, please remove them at the beginning: /Root/ExternalTable");
        }
    }

    Y_UNIT_TEST(DropNonExistingExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(/* nodeIndex */ 0).FeatureFlags.SetEnableExternalDataSources(/* value */ true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultSuccess = session.ExecuteSchemeQuery("DROP EXTERNAL DATA SOURCE test").GetValueSync();
        UNIT_ASSERT_C(resultSuccess.GetStatus() == EStatus::SCHEME_ERROR, TStringBuilder{} << resultSuccess.GetStatus() << " " << resultSuccess.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithOldSecretDisabled) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(/* value */ true);
        featureFlags.SetDisableOldSecretCreation(/* value */ true);
        featureFlags.SetDisableOldSecrets(/* value */ true);

        NKqp::TKikimrSettings settings;
        settings.SetFeatureFlags(featureFlags);
        settings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        static const auto query = R"sql(
            CREATE EXTERNAL DATA SOURCE `/Root/ExternalDataSource` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="mysa",
                SERVICE_ACCOUNT_SECRET_NAME="OldSecret"
            );
        )sql";
        const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(
            result.GetIssues().ToString(),
            "Old secrets are disabled for creating new objects. Please use new secrets",
            result.GetIssues().ToString());
    }
}

} // namespace NKikimr::NKqp
