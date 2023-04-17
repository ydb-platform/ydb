#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/client/minikql_result_lib/converter.h>
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

    std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = std::make_shared<TKqpTableMetadataLoader>(server.GetRuntime()->GetAnyNodeActorSystem(), false);
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

void CheckPolicies(Tests::TClient& client, const TString& tableName) {
    auto describeResult = client.Ls(tableName);
    UNIT_ASSERT(describeResult->Record.GetPathDescription().HasTableStats());
    const auto& desc = describeResult->Record.GetPathDescription();
    UNIT_ASSERT_VALUES_EQUAL(desc.GetTableStats().GetPartCount(), 4);
    for (const auto& column : desc.GetTable().GetColumns()) {
        if (column.GetName() == "Column2") {
            UNIT_ASSERT_VALUES_EQUAL(column.GetFamilyName(), "Family2");
        }
    }
    for (const auto& family : desc.GetTable().GetPartitionConfig().GetColumnFamilies()) {
        if (family.HasId() && family.GetId() == 0) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(family.GetColumnCodec()),
                static_cast<size_t>(NKikimrSchemeOp::ColumnCodecPlain));
        } else if (family.HasName() && family.GetName() == "Family2") {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(family.GetColumnCodec()),
                static_cast<size_t>(NKikimrSchemeOp::ColumnCodecLZ4));
        }
    }
}

struct TTestIndexSettings {
    const bool WithDataColumns;
};

void TestCreateTableCommon(TIntrusivePtr<IKikimrGateway> gateway, Tests::TClient& client,
        bool createFolders = true, const TMaybe<TTestIndexSettings> withIndex = Nothing(), bool withExtendedDdl = false,
        const TMaybe<bool>& shouldCreate = Nothing()) {
    auto metadata = MakeIntrusive<TKikimrTableMetadata>();

    metadata->Cluster = TestCluster;
    metadata->Name = "/Root/f1/f2/table";

    UNIT_ASSERT(metadata->ColumnOrder.size() == metadata->Columns.size());

    metadata->Columns.insert(std::make_pair("Column1", TKikimrColumnMetadata{"Column1", 0, "Uint32", false}));
    metadata->ColumnOrder.push_back("Column1");

    metadata->Columns.insert(std::make_pair("Column2", TKikimrColumnMetadata{"Column2", 0, "String", false}));
    metadata->ColumnOrder.push_back("Column2");

    if (withExtendedDdl) {
        metadata->Columns["Column2"].Families.push_back("Family2");
    }

    metadata->KeyColumnNames.push_back("Column1");

    if (withIndex) {
        TVector<TString> dataColumns;
        if (withIndex->WithDataColumns) {
            metadata->Columns.insert(std::make_pair("Column3", TKikimrColumnMetadata{"Column3", 0, "String", false}));
            metadata->ColumnOrder.push_back("Column3");
            dataColumns.push_back("Column3");
        }
        TIndexDescription indexDesc{
            TString("Column2Index"),
            TVector<TString>{"Column2"},
            dataColumns,
            TIndexDescription::EType::GlobalSync,
            TIndexDescription::EIndexState::Ready,
            0,
            0,
            0
        };
        metadata->Indexes.push_back(indexDesc);
    }

    if (withExtendedDdl) {
        metadata->TableSettings.AutoPartitioningBySize = "disabled";
        metadata->TableSettings.PartitionAtKeys = {
            {std::make_pair(EDataSlot::Uint32, "10")},
            {std::make_pair(EDataSlot::Uint32, "100")},
            {std::make_pair(EDataSlot::Uint32, "1000")}
        };
        metadata->ColumnFamilies = {
            {"default", "test", "off"},
            {"Family2", "test", "lz4"}
        };
    }

    auto responseFuture = gateway->CreateTable(metadata, createFolders);
    responseFuture.Wait();
    auto response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);
    if ((!shouldCreate && !createFolders) || (shouldCreate && !*shouldCreate)) {
        UNIT_ASSERT(!response.Success());
        UNIT_ASSERT(HasIssue(response.Issues(), TIssuesIds::KIKIMR_SCHEME_ERROR));
    } else {
        UNIT_ASSERT_C(response.Success(), response.Issues().ToString());

        auto loadFuture = gateway->LoadTableMetadata(TestCluster, "/Root/f1/f2/table",
            IKikimrGateway::TLoadTableMetadataSettings());
        loadFuture.Wait();
        auto loadResponse = loadFuture.GetValue();
        UNIT_ASSERT(loadResponse.Success());
        UNIT_ASSERT_VALUES_EQUAL(metadata->Name, loadResponse.Metadata->Name);
        UNIT_ASSERT_VALUES_EQUAL(metadata->Indexes.size(), loadResponse.Metadata->Indexes.size());

        THashMap<TString, TIndexDescription> expected;
        for (const auto& indexDesc : metadata->Indexes) {
            expected.insert(std::make_pair(indexDesc.Name, indexDesc));
        }

        THashMap<TString, TIndexDescription> indexResult;
        for (const auto& indexDesc : loadResponse.Metadata->Indexes) {
            indexResult.insert(std::make_pair(indexDesc.Name, indexDesc));
        }

        UNIT_ASSERT_VALUES_EQUAL(indexResult.size(), expected.size());
        for (const auto& indexDescResult : indexResult) {
            const auto expectedDesc = expected.find(indexDescResult.first);
            UNIT_ASSERT(expectedDesc != expected.end());
            UNIT_ASSERT_VALUES_EQUAL(expectedDesc->second.KeyColumns.size(), indexDescResult.second.KeyColumns.size());
            UNIT_ASSERT_EQUAL(expectedDesc->second.Type, indexDescResult.second.Type);
            for (size_t i = 0; i < indexDescResult.second.KeyColumns.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(indexDescResult.second.KeyColumns[i], expectedDesc->second.KeyColumns[i]);
            }
            UNIT_ASSERT_VALUES_EQUAL(expectedDesc->second.DataColumns.size(), indexDescResult.second.DataColumns.size());
            for (size_t i = 0; i < indexDescResult.second.DataColumns.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(indexDescResult.second.DataColumns[i], expectedDesc->second.DataColumns[i]);
            }
        }

        if (withExtendedDdl) {
            CheckPolicies(client, metadata->Name);
        }
    }
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
    NYql::TCreateExternalDataSourceSettings settings;
    settings.ExternalDataSource = path;
    settings.SourceType = "ObjectStorage";
    settings.AuthMethod = "NONE";
    settings.Installation = "cloud";

    auto responseFuture = gateway->CreateExternalDataSource(TestCluster, settings, true);
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
    auto responseFuture = gateway->DropExternalDataSource(TestCluster, TDropExternalDataSourceSettings{.ExternalDataSource=path});
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

    Y_UNIT_TEST(TestCreateTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient());
    }

    Y_UNIT_TEST(TestCreateTableWithIndex) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient(), true,
            TTestIndexSettings{false});
    }

    Y_UNIT_TEST(TestCreateTableWithCoverIndex) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient(), true,
            TTestIndexSettings{true});
    }

    Y_UNIT_TEST(TestCreateTableNoFolder) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient(), false, Nothing(),
            false, true);
    }

    Y_UNIT_TEST(TestCreateSameTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient());
    }

    Y_UNIT_TEST(TestCreateSameTableWithIndex) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient(), true,
            TTestIndexSettings{false});
    }

    Y_UNIT_TEST(TestDropTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestDropTableCommon(GetIcGateway(kikimr.GetTestServer()));
    }

    Y_UNIT_TEST(TestCreateTableWithExtendedDdl) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        CreateSampleTables(kikimr);
        TestCreateTableCommon(GetIcGateway(kikimr.GetTestServer()), kikimr.GetTestClient(), true, Nothing(), true);
    }

    Y_UNIT_TEST(TestCreateExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
    }

    Y_UNIT_TEST(TestCreateSameExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table", true);
    }

    Y_UNIT_TEST(TestDropExternalTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        TestCreateExternalDataSource(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_data_source");
        TestCreateExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
        TestDropExternalTable(*kikimr.GetTestServer().GetRuntime(), GetIcGateway(kikimr.GetTestServer()), "/Root/f1/f2/external_table");
    }

    Y_UNIT_TEST(TestDropExternalDataSource) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
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
}

} // namespace NYql
