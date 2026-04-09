#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;


Y_UNIT_TEST_SUITE(LocalityOperation) {
Y_UNIT_TEST(LocksFromAnotherTenants) {
    NKikimrConfig::TAppConfig appConfig;
    TKikimrWithGrpcAndRootSchema server(appConfig);
    //server.Server_->SetupLogging(

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

    TString first_tenant_name = "ydb_tenant_0";
    TString second_tenant_name = "ydb_tenant_1";

    {
        TClient admClient(*server.ServerSettings);
        for (auto& tenant_name: TVector<TString>{first_tenant_name, second_tenant_name}) {
            TString tenant_path = Sprintf("/Root/%s", tenant_name.c_str());

            TStoragePools tenant_pools = CreatePoolsForTenant(admClient, server.ServerSettings->StoragePoolTypes, tenant_path);

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     admClient.CreateSubdomain("/Root", GetSubDomainDeclarationSetting(tenant_name)));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                     admClient.AlterSubdomain("/Root", GetSubDomainDefaultSetting(tenant_name, tenant_pools), TDuration::MilliSeconds(500)));

            server.Tenants_->Run(tenant_path, 1);
        }
    }

    NYdb::NTable::TTableClient client(connection);

    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    for (auto& tenant_name: TVector<TString>{first_tenant_name, second_tenant_name}) {
        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::Utf8);
        tableBuilder.SetPrimaryKeyColumn("Key");

        TString table_path = Sprintf("/Root/%s/table", tenant_name.c_str());

        auto result = session.CreateTable(table_path, tableBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) VALUES (1u, \"One\");", first_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetStatus() << result.GetIssues().ToString());
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) VALUES (2u, \"Second\");", second_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                            "Status: " << result.GetStatus()
                                << " Issues: " << result.GetIssues().ToString());
    }

    {
        TString query = Sprintf("UPSERT INTO `Root/%s/table` (Key, Value) SELECT Key, Value FROM `Root/%s/table`;", second_tenant_name.c_str(), first_tenant_name.c_str());
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::CANCELLED,
                            "Status: " << result.GetStatus()
                                       << " Issues: " << result.GetIssues().ToString());
    }
}
}

} // namespace NKikimr

