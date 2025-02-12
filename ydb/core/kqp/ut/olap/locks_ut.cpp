#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapLocks) {
    Y_UNIT_TEST(TwoQueriesWithRestartTablet) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        //        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_TX, NActors::NLog::PRI_DEBUG);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        csController->SetInterruptionOnLockedTransactions(true);
        auto prepareResultFuture1 =
            client.ExecuteQuery(R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, "test1", 10), (3u, "test3", 13);)",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        auto prepareResultFuture2 =
            client.ExecuteQuery(R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(2u, "test2", 11), (4u, "test4", 14);)",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        Sleep(TDuration::Seconds(1));
        for (auto&& i : csController->GetShardActualIds()) {
            kikimr.GetTestServer().GetRuntime()->Send(
                MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
        }
        Sleep(TDuration::Seconds(10));
        csController->SetInterruptionOnLockedTransactions(false);
        auto prepareResult1 = prepareResultFuture1.ExtractValueSync();
        auto prepareResult2 = prepareResultFuture2.ExtractValueSync();
        //        UNIT_ASSERT_C(prepareResult1.IsSuccess(), prepareResult1.GetIssues().ToString());
        //        UNIT_ASSERT_C(prepareResult2.IsSuccess(), prepareResult2.GetIssues().ToString());

        {
            auto it =
                client.StreamExecuteQuery("SELECT * FROM `/Root/ColumnTable` ORDER BY Col1", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["test1"];10];[2u;["test2"];11];[3u;["test3"];13];[4u;["test4"];14]])");
        }
    }
    Y_UNIT_TEST(TableSinkWithOlapStore) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD_WRITE, NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();

        TLocalHelper(kikimr).CreateTestOlapTables();

        WriteTestData(kikimr, "/Root/olapStore/olapTable0", 0, 1000000, 3, true);

        auto client = kikimr.GetQueryClient();
        {
            auto result = client
                              .ExecuteQuery(R"(
                SELECT * FROM `/Root/olapStore/olapTable0` ORDER BY timestamp;
                INSERT INTO `/Root/olapStore/olapTable1` SELECT * FROM `/Root/olapStore/olapTable0`;
                REPLACE INTO `/Root/olapStore/olapTable0` SELECT * FROM `/Root/olapStore/olapTable1`;
                SELECT * FROM `/Root/olapStore/olapTable1` ORDER BY timestamp;
            )",
                                  NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    void TestDeleteAbsent(const size_t shardCount, bool reboot) {
        //This test tries to DELETE from a table when there is no rows to delete at some shard
        //It corresponds to a SCAN, then NO write then COMMIT on that shard
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        NKikimrConfig::TAppConfig appConfig;
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(false);
        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Int32).SetNullable(true),
        };
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ttt").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema).SetMinPartitionsCount(shardCount);
        testHelper.CreateTable(testTable);
        auto client = testHelper.GetKikimr().GetQueryClient();
        //1. Insert exactlly one row into a table, so the only shard will contain a row
        const auto result = client
                                .ExecuteQuery(
                                    R"(
                 INSERT INTO `/Root/ttt` (id, value) VALUES
                 (1, 11)
                )",
                                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        //2. Ensure that there is actually 1 row in the table
        {
            const auto resultSelect =
                client.ExecuteQuery("SELECT * FROM `/Root/ttt`", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
            const auto resultSets = resultSelect.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
            const auto resultSet = resultSets[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        }
        if (reboot) {
            csController->SetRestartOnLocalTxCommitted("TProposeWriteTransaction");
        }
        //DELETE 1 row from one shard and 0 rows from others
        const auto resultDelete = client.ExecuteQuery("DELETE from `/Root/ttt` ", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(resultDelete.IsSuccess() != reboot, resultDelete.GetIssues().ToString());
        {
            const auto resultSelect =
                client.ExecuteQuery("SELECT * FROM `/Root/ttt`", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
            const auto resultSets = resultSelect.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
            const auto resultSet = resultSets[0];
            if (shardCount > 1 && reboot) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1); // locks broken
            } else {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0); // not need locks
            }
        }
        //DELETE 0 rows from every shard
        const auto resultDelete2 =
            client.ExecuteQuery("DELETE from `/Root/ttt` WHERE id < 100", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        if (shardCount > 1 && reboot) {
            UNIT_ASSERT_C(!resultDelete2.IsSuccess(), result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_C(resultDelete2.IsSuccess(), result.GetIssues().ToString());
        }
    }
    Y_UNIT_TEST_TWIN(DeleteAbsentSingleShard, Reboot) {
        TestDeleteAbsent(1, Reboot);
    }

    Y_UNIT_TEST_TWIN(DeleteAbsentMultipleShards, Reboot) {
        TestDeleteAbsent(2, Reboot);
    }
}

}   // namespace NKikimr::NKqp
