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

Y_UNIT_TEST_SUITE(KqpOlapJson) {
    Y_UNIT_TEST(Simple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        //        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_TX, NActors::NLog::PRI_DEBUG);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        {
            const TString query = R"(
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`);)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();
        auto prepareResult1 =
            client.ExecuteQuery(R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'));)",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                .ExtractValueSync();
        UNIT_ASSERT_C(prepareResult1.IsSuccess(), prepareResult1.GetIssues().ToString());

        {
            auto it =
                client.StreamExecuteQuery("SELECT * FROM `/Root/ColumnTable` ORDER BY Col1", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["test1"];10];[2u;["test2"];11];[3u;["test3"];13];[4u;["test4"];14]])");
        }
    }

    Y_UNIT_TEST(Merge) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_TX, NActors::NLog::PRI_DEBUG);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        {
            const TString query = R"(
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query =
                R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`);)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult =
                client
                    .ExecuteQuery(R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'));)",
                        NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult =
                client
                    .ExecuteQuery(R"(REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a" : "1a1"}')), (12u, JsonDocument('{"a" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b" : "1b3"}')), (14u, JsonDocument('{"b" : "1b4", "a" : "a4"}'));)",
                        NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        csController->WaitCompactions(TDuration::Seconds(5));

        {
            auto it =
                client.StreamExecuteQuery("SELECT * FROM `/Root/ColumnTable` ORDER BY Col1", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["test1"];10];[2u;["test2"];11];[3u;["test3"];13];[4u;["test4"];14]])");
        }
    }
}

}   // namespace NKikimr::NKqp
