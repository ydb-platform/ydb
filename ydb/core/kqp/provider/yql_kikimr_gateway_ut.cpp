#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/client/minikql_result_lib/converter.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/generic/maybe.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NMiniKQL;
using namespace NResultLib;
using namespace NYdb::NTable;

namespace {

constexpr const char* TestCluster = "kikimr";

void CreateSampleTables(TKikimrRunner& runner) {
    auto schemeClient = runner.GetSchemeClient();
    AssertSuccessResult(schemeClient.MakeDirectory("/Root/Test").GetValueSync());
    AssertSuccessResult(schemeClient.MakeDirectory("/Root/Test/UserDir").GetValueSync());

    auto tableClient = runner.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    AssertSuccessResult(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/Test/UserTable` (
            UserKey Utf8,
            UserSubkey Uint32,
            UserValue Utf8,
            PRIMARY KEY (UserKey, UserSubkey)
        );
        CREATE TABLE `/Root/Test/TestTable2` (
            Group Uint32,
            Name String,
            Amount Uint64,
            Comment String,
            PRIMARY KEY (Group, Name)
        );
        CREATE TABLE `/Root/Test/TestTableKsv` (
            key String,
            subkey String,
            value String,
            PRIMARY KEY (key, subkey)
        );
        CREATE TABLE `/Root/Test/TestTable3` (
            Key Utf8,
            SomeJson Json,
            PRIMARY KEY (Key)
        );
    )").GetValueSync());

    AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/Test/TestTable2` (Group, Name, Amount, Comment) VALUES
            (1u, "Anna", 3500ul, "None"),
            (1u, "Paul", 300ul, "None"),
            (2u, "Tony", 7200ul, "None");

        REPLACE INTO `/Root/Test/TestTableKsv` (key, subkey, value) VALUES
            ("Anna", "1", "Value1"),
            ("Anna", "2", "Value2"),
            ("Paul", "1", "Value3"),
            ("Tony", "2", "Value4");
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());
}

TIntrusivePtr<IKqpGateway> GetIcGateway(Tests::TServer& server) {
    auto counters = MakeIntrusive<TKqpRequestCounters>();
    counters->Counters = new TKqpCounters(server.GetRuntime()->GetAppData(0).Counters);
    counters->TxProxyMon = new NTxProxy::TTxProxyMon(server.GetRuntime()->GetAppData(0).Counters);

    std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = std::make_shared<TKqpTableMetadataLoader>(server.GetRuntime()->GetAnyNodeActorSystem(),TIntrusivePtr<NYql::TKikimrConfiguration>(nullptr), false);
    return CreateKikimrIcGateway(TestCluster, "/Root", std::move(loader), server.GetRuntime()->GetAnyNodeActorSystem(),
        server.GetRuntime()->GetNodeId(0), counters);
}

void TestListPathCommon(TIntrusivePtr<IKikimrGateway> gateway) {
    auto responseFuture = gateway->ListPath(TestCluster, "/Root/Test");
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    UNIT_ASSERT(response.Success());

    UNIT_ASSERT_VALUES_EQUAL(response.Path, "/Root/Test");
    UNIT_ASSERT_VALUES_EQUAL(response.Items.size(), 5);
    UNIT_ASSERT_VALUES_EQUAL(response.Items[0].Name, "TestTable2");
    UNIT_ASSERT_VALUES_EQUAL(response.Items[0].IsDirectory, false);
    UNIT_ASSERT_VALUES_EQUAL(response.Items[1].Name, "TestTable3");
    UNIT_ASSERT_VALUES_EQUAL(response.Items[1].IsDirectory, false);
    UNIT_ASSERT_VALUES_EQUAL(response.Items[2].Name, "TestTableKsv");
    UNIT_ASSERT_VALUES_EQUAL(response.Items[2].IsDirectory, false);
    UNIT_ASSERT_VALUES_EQUAL(response.Items[3].Name, "UserDir");
    UNIT_ASSERT_VALUES_EQUAL(response.Items[3].IsDirectory, true);
    UNIT_ASSERT_VALUES_EQUAL(response.Items[4].Name, "UserTable");
    UNIT_ASSERT_VALUES_EQUAL(response.Items[4].IsDirectory, false);
}

void TestLoadTableMetadataCommon(TIntrusivePtr<IKikimrGateway> gateway) {
    auto responseFuture = gateway->LoadTableMetadata(TestCluster, "/Root/Test/UserTable",
        IKikimrGateway::TLoadTableMetadataSettings());
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    UNIT_ASSERT(response.Success());

    auto& metadata = *response.Metadata;
    UNIT_ASSERT_VALUES_EQUAL(metadata.Cluster, TestCluster);
    UNIT_ASSERT_VALUES_EQUAL(metadata.Name, "/Root/Test/UserTable");
    UNIT_ASSERT_VALUES_EQUAL(metadata.Columns.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(metadata.Columns["UserKey"].Type, "Utf8");
    UNIT_ASSERT_VALUES_EQUAL(metadata.Columns["UserSubkey"].Type, "Uint32");
    UNIT_ASSERT_VALUES_EQUAL(metadata.Columns["UserValue"].Type, "Utf8");
    UNIT_ASSERT_VALUES_EQUAL(metadata.KeyColumnNames.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(metadata.KeyColumnNames[0], "UserKey");
    UNIT_ASSERT_VALUES_EQUAL(metadata.KeyColumnNames[1], "UserSubkey");
}

void TestDropTableCommon(TIntrusivePtr<IKikimrGateway> gateway) {
    auto responseFuture = gateway->DropTable(TestCluster, "/Root/Test/UserTable");
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    UNIT_ASSERT(response.Success());

    auto loadFuture = gateway->LoadTableMetadata(TestCluster, "/Root/Test/UserTable",
        IKikimrGateway::TLoadTableMetadataSettings());
    loadFuture.Wait();
    auto loadResponse = loadFuture.GetValue();
    UNIT_ASSERT(loadResponse.Success());
    UNIT_ASSERT(!loadResponse.Metadata->DoesExist);
}

void TestCreateExternalDataSource(TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TString& path) {
    TCreateObjectSettings settings("EXTERNAL_DATA_SOURCE", path, {
        {"source_type", "ObjectStorage"},
        {"auth_method", "NONE"},
        {"installation", "cloud"}
    });
    auto responseFuture = gateway->CreateObject(TestCluster, settings);
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);

    UNIT_ASSERT_C(response.Success(), response.Issues().ToString());

    auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
    UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
    UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
    UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "cloud");
    UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "");
    UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(path).back());
    UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasNone());
}

void TestCreateExternalTable(TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TString& path, bool fail = false) {
    NYql::TCreateExternalTableSettings settings;
    settings.ExternalTable = path;
    settings.DataSourcePath = "/Root/f1/f2/external_data_source";
    settings.Location = "/";

    settings.Columns.insert(std::make_pair("Column1", TKikimrColumnMetadata{"Column1", 0, "Uint32", false}));
    settings.ColumnOrder.push_back("Column1");

    settings.Columns.insert(std::make_pair("Column2", TKikimrColumnMetadata{"Column2", 0, "String", false}));
    settings.ColumnOrder.push_back("Column2");

    auto responseFuture = gateway->CreateExternalTable(TestCluster, settings, true);
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);

    if (fail) {
        UNIT_ASSERT_C(!response.Success(), response.Issues().ToString());
        return;
    }

    UNIT_ASSERT_C(response.Success(), response.Issues().ToString());

    auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& externalTable = externalTableDesc->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(externalTable.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
    UNIT_ASSERT(externalTable.ExternalTableInfo);
    UNIT_ASSERT_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
}

void TestDropExternalTable(TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TString& path) {
    auto responseFuture = gateway->DropExternalTable(TestCluster, TDropExternalTableSettings{.ExternalTable=path});
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    UNIT_ASSERT(response.Success());

    auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& externalTable = externalTableDesc->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(externalTableDesc->ErrorCount, 1);
    UNIT_ASSERT_EQUAL(externalTable.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
}

void TestDropExternalDataSource(TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TString& path) {
    TDropObjectSettings settings("EXTERNAL_DATA_SOURCE", path, {});
    auto responseFuture = gateway->DropObject(TestCluster, settings);
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    UNIT_ASSERT(response.Success());

    auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(externalDataSourceDesc->ErrorCount, 1);
    UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
}

} // namespace


Y_UNIT_TEST_SUITE(KikimrIcGateway) {
    Y_UNIT_TEST(TestListPath) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestListPathCommon(GetIcGateway(kikimr.GetTestServer()));
    }

    Y_UNIT_TEST(TestLoadTableMetadata) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestLoadTableMetadataCommon(GetIcGateway(kikimr.GetTestServer()));
    }

    Y_UNIT_TEST(TestDropTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestDropTableCommon(GetIcGateway(kikimr.GetTestServer()));
    }

    Y_UNIT_TEST(TestCreateExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
    }

    Y_UNIT_TEST(TestCreateSameExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table", true);
    }

    Y_UNIT_TEST(TestDropExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
        TestDropExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
    }

    Y_UNIT_TEST(TestDropExternalDataSource) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestDropExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
    }

    Y_UNIT_TEST(TestLoadExternalTable) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
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
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        auto responseFuture = GetIcGateway(kikimr.GetTestServer())->LoadTableMetadata(TestCluster, externalTableName, IKikimrGateway::TLoadTableMetadataSettings());
        responseFuture.Wait();
        auto response = responseFuture.GetValue();
        response.Issues().PrintTo(Cerr);
        UNIT_ASSERT(response.Success());
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.Type, "ObjectStorage");
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.TableLocation, "/");
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.DataSourcePath, externalDataSourceName);
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.DataSourceLocation, "my-bucket");
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->Columns.size(), 2);
    }

    void CreateSecretObject(const TString& secretId, const TString& secretValue, TSession& session, TTestActorRuntime* runtime) {
        auto createSecretQuery = TStringBuilder() << "CREATE OBJECT " << secretId << " (TYPE SECRET) WITH value = `" << secretValue << "`;";
        auto createSecretQueryResult = session.ExecuteSchemeQuery(createSecretQuery).GetValueSync();
        UNIT_ASSERT_C(createSecretQueryResult.GetStatus() == NYdb::EStatus::SUCCESS, createSecretQueryResult.GetIssues().ToString());

        TDuration maximalWaitTime = TDuration::Seconds(20);
        TInstant start = TInstant::Now();
        bool created = false;
        while (!created && TInstant::Now() - start <= maximalWaitTime) {
            auto promise = NThreading::NewPromise<TDescribeSecretsResponse>();
            runtime->Register(new TDescribeSecretsActor("", {secretId}, promise));
            TDescribeSecretsResponse response = promise.GetFuture().GetValueSync();

            if (response.Status == Ydb::StatusIds::SUCCESS) {
                created = true;
                break;
            }
            
            Sleep(TDuration::Seconds(2));
        }

        UNIT_ASSERT_C(created, "Creating secret object timeout.\n");
    }
    
    Y_UNIT_TEST(TestLoadServiceAccountSecretValueFromExternalDataSourceMetadata) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretId = "mySaSecretId";
        TString secretValue = "mySaSecretValue";
        CreateSecretObject(secretId, secretValue, session, kikimr.GetTestServer().GetRuntime());

        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="",
                SERVICE_ACCOUNT_SECRET_NAME=")" << secretId << R"("
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto responseFuture = GetIcGateway(kikimr.GetTestServer())->LoadTableMetadata(TestCluster, externalTableName, IKikimrGateway::TLoadTableMetadataSettings());
        responseFuture.Wait();

        auto response = responseFuture.GetValue();
        UNIT_ASSERT_C(response.Success(), response.Issues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.ServiceAccountIdSignature, secretValue);
    }

    Y_UNIT_TEST(TestLoadBasicSecretValueFromExternalDataSourceMetadata) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretId = "myPasswordSecretId";
        TString secretValue = "pswd";
        CreateSecretObject(secretId, secretValue, session, kikimr.GetTestServer().GetRuntime());

        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="PostgreSQL",
                LOCATION="my-bucket",
                AUTH_METHOD="BASIC",
                LOGIN="mylogin",
                PASSWORD_SECRET_NAME=")" << secretId << R"("
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto responseFuture = GetIcGateway(kikimr.GetTestServer())->LoadTableMetadata(TestCluster, externalDataSourceName, IKikimrGateway::TLoadTableMetadataSettings());
        responseFuture.Wait();

        auto response = responseFuture.GetValue();
        UNIT_ASSERT_C(response.Success(), response.Issues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.Password, secretValue);
    }

    Y_UNIT_TEST(TestLoadMdbBasicSecretValueFromExternalDataSourceMetadata) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretPasswordId = "myPasswordSecretId";
        TString secretPasswordValue = "pswd";
        CreateSecretObject(secretPasswordId, secretPasswordValue, session, kikimr.GetTestServer().GetRuntime());

        TString secretSaId = "mySa";
        TString secretSaValue = "sign(mySa)";
        CreateSecretObject(secretSaId, secretSaValue, session, kikimr.GetTestServer().GetRuntime());

        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="PostgreSQL",
                LOCATION="my-bucket",
                AUTH_METHOD="MDB_BASIC",
                SERVICE_ACCOUNT_ID="mysa",
                SERVICE_ACCOUNT_SECRET_NAME=")" << secretSaId << R"(",
                LOGIN="mylogin",
                PASSWORD_SECRET_NAME=")" << secretPasswordId << R"("
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto responseFuture = GetIcGateway(kikimr.GetTestServer())->LoadTableMetadata(TestCluster, externalDataSourceName, IKikimrGateway::TLoadTableMetadataSettings());
        responseFuture.Wait();

        auto response = responseFuture.GetValue();
        UNIT_ASSERT_C(response.Success(), response.Issues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.Password, secretPasswordValue);
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.ServiceAccountIdSignature, secretSaValue);
    }

     Y_UNIT_TEST(TestLoadAwsSecretValueFromExternalDataSourceMetadata) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString awsAccessKeyIdSecretId = "awsAccessKeyIdSecretId";
        TString awsAccessKeyIdSecretValue = "key";
        CreateSecretObject(awsAccessKeyIdSecretId, awsAccessKeyIdSecretValue, session, kikimr.GetTestServer().GetRuntime());

        TString awsSecretAccessKeySecretId = "awsSecretAccessKeySecretId";
        TString awsSecretAccessKeySecretValue = "value";
        CreateSecretObject(awsSecretAccessKeySecretId, awsSecretAccessKeySecretValue, session, kikimr.GetTestServer().GetRuntime());

        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME=")" << awsAccessKeyIdSecretId << R"(",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME=")" << awsSecretAccessKeySecretId << R"("
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto responseFuture = GetIcGateway(kikimr.GetTestServer())->LoadTableMetadata(TestCluster, externalDataSourceName, IKikimrGateway::TLoadTableMetadataSettings());
        responseFuture.Wait();

        auto response = responseFuture.GetValue();
        UNIT_ASSERT_C(response.Success(), response.Issues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.AwsAccessKeyId, awsAccessKeyIdSecretValue);
        UNIT_ASSERT_VALUES_EQUAL(response.Metadata->ExternalSource.AwsSecretAccessKey, awsSecretAccessKeySecretValue);
    }
}

} // namespace NYql
