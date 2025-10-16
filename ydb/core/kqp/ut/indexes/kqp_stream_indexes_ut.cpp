#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
//#include <ydb/core/tx/data_events/events.h> 
//#include <ydb/core/tx/datashard/datashard.h>
//#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpStreamIndexes) {
    void RunTest(TString query, TString mainResult, TString indexResult, bool exists) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                Col1 Uint64,
                Col2 Uint64,
                Col3 Uint64,
                INDEX idx GLOBAL ON (Col2),
                PRIMARY KEY (Col1)
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        if (exists) {
            auto it = client.ExecuteQuery(
                "INSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 1u, 1u);",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto it = client.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2, Col3 FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, mainResult);
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/DataShard/idx/indexImplTable`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, indexResult);
        }
    }

    Y_UNIT_TEST(InsertFull) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            false);
    }

    Y_UNIT_TEST_TWIN(ReplaceFull, exists) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            exists);
    }

    Y_UNIT_TEST_TWIN(UpsertFull, exists) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            exists);
    }

    Y_UNIT_TEST(InsertNoColumn) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            false);
    }

    Y_UNIT_TEST(InsertNoColumn2) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            false);
    }

    Y_UNIT_TEST_TWIN(ReplaceNoColumn, exists) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            exists);
    }

    Y_UNIT_TEST_TWIN(ReplaceNoColumn2, exists) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            exists);
    }

    Y_UNIT_TEST(UpsertNoColumnNotExists) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            false);
    }

    Y_UNIT_TEST(UpsertNoColumnExists) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true);
    }

    Y_UNIT_TEST(UpsertNoColumnNotExists2) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            false);
    }

    Y_UNIT_TEST(UpsertNoColumnExists2) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true);
    }

    Y_UNIT_TEST_TWIN(DeleteKey, exists) {
        RunTest(
            "DELETE FROM `/Root/DataShard` WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            exists);
    }

    Y_UNIT_TEST_TWIN(DeleteKey2, exists) {
        RunTest(
            "DELETE FROM `/Root/DataShard` ON SELECT 0u AS Col1;",
            R"([])",
            R"([])",
            exists);
    }

    Y_UNIT_TEST(UpdateKeyExists) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col2 = 0u WHERE Col1 = 0u;",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true);
    }

    Y_UNIT_TEST(UpdateKeyNotExists) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col2 = 0u WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            false);
    }

    Y_UNIT_TEST(UpdateKeyExists2) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col3 = 0u WHERE Col1 = 0u;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true);
    }

    Y_UNIT_TEST(UpdateKeyNotExists2) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col3 = 0u WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            false);
    }

    Y_UNIT_TEST(UpdateKeyExists3) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col3;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true);
    }

    Y_UNIT_TEST(UpdateKeyNotExists3) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col3;",
            R"([])",
            R"([])",
            false);
    }

    Y_UNIT_TEST(UpdateKeyExists4) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col2;",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true);
    }

    Y_UNIT_TEST(UpdateKeyNotExists4) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col2;",
            R"([])",
            R"([])",
            false);
    }
}
}
}
