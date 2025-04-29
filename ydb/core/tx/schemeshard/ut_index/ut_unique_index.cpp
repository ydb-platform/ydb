#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NYdb::Dev::NTable;

Y_UNIT_TEST_SUITE(TUniqueIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableUniqConstraint(true);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
              Type: EIndexTypeGlobalUnique
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex"),
            {NLs::PathExist,
             NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
             NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
             NLs::IndexKeys({"indexed"})});
    }

    Y_UNIT_TEST(CreateTableByTableClient) {
      NKikimrConfig::TAppConfig appConfig;
      NYdb::TKikimrWithGrpcAndRootSchema server{std::move(appConfig)};

      auto driver = NYdb::TDriver(NYdb::TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
      NYdb::NTable::TTableClient tableClient(driver);
      auto session = tableClient.GetSession().ExtractValueSync().GetSession();
      constexpr const char* table = "/Root/table";
      constexpr const char* index = "byValue";

      TString query = Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint32,
            Group Uint32,
            Value Uint32,
            PRIMARY KEY (Key),
            INDEX %s GLOBAL UNIQUE ON (Value)
          );
      )", table, index);

      session.ExecuteSchemeQuery(query).ExtractValueSync();

      auto desc = session.DescribeTable(table).ExtractValueSync().GetTableDescription();
      UNIT_ASSERT_EQUAL(desc.GetIndexDescriptions().size(), 1);
      UNIT_ASSERT_EQUAL(desc.GetIndexDescriptions().at(0).GetIndexName(), index);
      // TODO: Remove the flag after fixing the creation of unique indexes by table client
      bool isFixedCreationUniqueIndexes = false;
      if (!isFixedCreationUniqueIndexes) {
        UNIT_ASSERT_EQUAL(desc.GetIndexDescriptions().at(0).GetIndexType(), EIndexType::GlobalSync);
      } else {
        UNIT_ASSERT_EQUAL(desc.GetIndexDescriptions().at(0).GetIndexType(), EIndexType::GlobalUnique);
      }
  }
}
