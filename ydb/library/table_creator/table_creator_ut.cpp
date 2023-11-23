#include "table_creator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TableCreator) {
    Y_UNIT_TEST(CreateTable) {
        TPortManager tp;
        ui16 mbusPort = tp.GetPort();
        ui16 grpcPort = tp.GetPort();
        auto settings = Tests::TServerSettings(mbusPort);
        settings.SetNodeCount(1);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.EnableGRpc(grpcPort);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        TVector<TString> pathComponents = {"path", "to", "table"};

        TVector<NKikimrSchemeOp::TColumnDescription> columns;
        NKikimrSchemeOp::TColumnDescription descKey;
        descKey.SetName("key");
        descKey.SetType("Uint32");
        columns.push_back(descKey);
        NKikimrSchemeOp::TColumnDescription descValue;
        descValue.SetName("value");
        descValue.SetType("String");
        columns.push_back(descValue);

        TVector<TString> keyColumns = {"key"};

        TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor(0);
        runtime->Register(CreateTableCreator(
            std::move(pathComponents), std::move(columns), std::move(keyColumns), NKikimrServices::STATISTICS),
            0, 0, TMailboxType::Simple, 0, edgeActor);

        runtime->GrabEdgeEvent<TEvTableCreator::TEvCreateTableResponse>(edgeActor);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << grpcPort).SetDatabase(Tests::TestDomainName);
        NYdb::TDriver driver(cfg);
        NYdb::NTable::TTableClient tableClient(driver);
        auto createSessionResult = tableClient.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
        NYdb::NTable::TSession session(createSessionResult.GetSession());

        TString path = TString("/") + Tests::TestDomainName + "/path/to/table";
        auto result = session.DescribeTable(path).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& createdColumns = result.GetTableDescription().GetColumns();
        UNIT_ASSERT_C(createdColumns.size() == 2, "expected 2 columns");
        UNIT_ASSERT_C(createdColumns[0].Name == "key", "expected key column");
        UNIT_ASSERT_C(createdColumns[1].Name == "value", "expected value column");
    }
}

} // namespace NKikimr
