#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
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
        auto prepareResultFuture1 = client.ExecuteQuery(
            R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, "test1", 10), (3u, "test3", 13);)", NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        auto prepareResultFuture2 = client.ExecuteQuery(
            R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(2u, "test2", 11), (4u, "test4", 14);)", NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        Sleep(TDuration::Seconds(1));
        for (auto&& i : csController->GetShardActualIds()) {
            kikimr.GetTestServer().GetRuntime()->Send(
                MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
        }
        Sleep(TDuration::Seconds(10));
        csController->SetInterruptionOnLockedTransactions(false);
        auto prepareResult1 = prepareResultFuture1.ExtractValueSync();
        auto prepareResult2 = prepareResultFuture2.ExtractValueSync();
        UNIT_ASSERT_C(prepareResult1.IsSuccess(), prepareResult1.GetIssues().ToString());
        UNIT_ASSERT_C(prepareResult2.IsSuccess(), prepareResult2.GetIssues().ToString());

        {
            auto it =
                client.StreamExecuteQuery("SELECT * FROM `/Root/ColumnTable` ORDER BY Col1", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["test1"];10];[2u;["test2"];11]])");
        }
    }
}

}   // namespace NKikimr::NKqp
