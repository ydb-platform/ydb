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

}

}   // namespace NKikimr::NKqp