#include <ut/common/kqp_ut_common.h>
#include <ut/olap/helpers/local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/services/ydb/ydb_common_ut.h>

namespace NKikimr::NKqp {

NKikimrConfig::TAppConfig GetAppConfig() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableColumnStore(true);
    return appConfig;
}

Y_UNIT_TEST_SUITE(NamingValidation) {

    Y_UNIT_TEST(CreateColumnTableOk) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);

        TString tableName = "test";
        TString tableDescr = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "message"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateColumnTable("/Root", tableDescr);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    Y_UNIT_TEST(CreateColumnTableFailed) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);

        TString tableDescr = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "mess age"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateColumnTable("/Root", tableDescr);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_ERROR);
    }

    Y_UNIT_TEST(CreateColumnStoreOk) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);
        
        TString tableDescr = Sprintf(R"(
            Name: "OlapStore"
            ColumnShardCount: 4
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "message" Type: "Utf8" }
                    Columns { Name: "id" Type: "Int32" NotNull: True }
                    KeyColumnNames: ["id"]
                }
            }
        )");

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateOlapStore("/Root", tableDescr);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);

        status = annoyingClient.CreateColumnTable("/Root/OlapStore", R"(
            Name: "Test"
            ColumnShardCount : 4
        )");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    Y_UNIT_TEST(CreateColumnStoreFailed) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);
        
        TString tableDescr = Sprintf(R"(
            Name: "OlapStore"
            ColumnShardCount: 4
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "mess age" Type: "Utf8" }
                    Columns { Name: "id" Type: "Int32" NotNull: True }
                    KeyColumnNames: ["id"]
                }
            }
        )");

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateOlapStore("/Root", tableDescr);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_ERROR);
    }

    Y_UNIT_TEST(AlterColumnTableOk) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);

        TString tableName = "test";
        TString tableDescr = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "message"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateColumnTable("/Root", tableDescr);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);

        TString alter = R"(
            Name: "TestTable"
            Columns {
                Name: "NewColumn"
                Type: "Int32"
            }
        )";

        status = annoyingClient.AlterTable("/Root", alter);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    Y_UNIT_TEST(AlterColumnTableFailed) {
        NYdb::TKikimrWithGrpcAndRootSchema server(GetAppConfig());
        Tests::TClient annoyingClient(*server.ServerSettings);

        TString tableName = "test";
        TString tableDescr = R"(
            Name: "TestTable"
            Schema {
                Columns {
                    Name: "Id"
                    Type: "Int32"
                    NotNull: True
                }
                Columns {
                    Name: "message"
                    Type: "Utf8"
                }
                KeyColumnNames: ["Id"]
            }
        )";

        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateColumnTable("/Root", tableDescr);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);

        TString alter = R"(
            Name: "TestTable"
            Columns {
                Name: "New Column"
                Type: "Int32"
            }
        )";

        status = annoyingClient.AlterTable("/Root", alter);

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_ERROR);
    }
    
    Y_UNIT_TEST(AlterColumnStoreOk) {
        auto settings = TKikimrSettings();
        TKikimrRunner kikimr(settings);
        TLocalHelper testHelper(kikimr);

        testHelper.CreateTestOlapTable("TestTable");
        auto tableClient = kikimr.GetTableClient();

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column1 Uint64;";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterColumnStoreFailed) {
        auto settings = TKikimrSettings();
        TKikimrRunner kikimr(settings);
        TLocalHelper testHelper(kikimr);

        testHelper.CreateTestOlapTable("TestTable");
        auto tableClient = kikimr.GetTableClient();

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `/Root/olapStore` ADD COLUMN `new column1` Uint64;";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SCHEME_ERROR, alterResult.GetIssues().ToString());
        }
    }
}

}   // namespace NKikimr::NKqp