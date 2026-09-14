#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TUniqueIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

    Y_UNIT_TEST_FLAG(PersistUniqueIndexKeySize, OnCreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnableAddUniqueIndex(true);
        appData.FeatureFlags.SetEnableOnlineAddUniqueIndex(true);
        ui64 txId = 100;

        if (OnCreate) {
            TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "uniqkey" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                }
                IndexDescription {
                    Name: "idx_uniq"
                    KeyColumnNames: ["uniqkey"]
                    Type: EIndexTypeGlobalUnique
                }
            )");
            env.TestWaitNotification(runtime, txId);
        } else {
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "uniqkey" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);
            TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
                "idx_uniq", NKikimrSchemeOp::EIndexTypeGlobalUnique, {"uniqkey"}, {}, {}
            });
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/idx_uniq"),
            {NLs::PathExist,
             NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
             NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
             NLs::IndexKeys({"uniqkey"})});

        Cerr << "rebooting schemeshard\n";
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Split the index table
        {
            auto indexDesc = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table/idx_uniq/indexImplTable", true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 1);
            TestSplitTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/Table/idx_uniq/indexImplTable", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
            )", parts[0].GetDatashardId()));
            env.TestWaitNotification(runtime, txId);
        }

        // Verify UniqueIndexKeySize at datashards
        {
            auto indexDesc = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table/idx_uniq/indexImplTable", true, true, true);
            auto parts = indexDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_EQUAL(parts.size(), 2);
            for (auto& x: parts) {
                auto edge = runtime.AllocateEdgeActor();
                runtime.SendToPipe(x.GetDatashardId(), edge, new TEvDataShard::TEvGetInfoRequest(), 0, GetPipeConfigWithRetries());
                TAutoPtr<IEventHandle> handle;
                auto* resp = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
                UNIT_ASSERT(resp);
                UNIT_ASSERT_VALUES_EQUAL(resp->Record.UserTablesSize(), 1u);
                const auto& descr = resp->Record.GetUserTables(0).GetDescription();
                UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().GetUniqueIndexKeySize(), 1u);
            }
        }
    }
}
