#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpStreamIndexes) {
    void RunTest(TString query, TString mainResult, TString indexResult, bool exists, bool indexOverlap, bool pkOverlap = false, bool cover = false) {
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
                INDEX idx GLOBAL ON (Col2 %s) %s,
                PRIMARY KEY (Col1 %s)
            );
        )", indexOverlap ? ", Col1" : "",  cover ? "COVER (Col3)" : "", pkOverlap ? ", Col2" : "");

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
                SELECT * FROM `/Root/DataShard/idx/indexImplTable`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, indexResult);
        }
    }

    Y_UNIT_TEST_TWIN(InsertFull, overlap) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            false,
            overlap);
    }

    Y_UNIT_TEST_QUAD(ReplaceFull, exists, overlap) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_QUAD(UpsertFull, exists, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (0u, 0u, 0u);",
            R"([[[0u];[0u];[0u]]])",
            R"([[[0u];[0u]]])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_TWIN(InsertNoColumn, overlap) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(InsertNoColumn2, overlap) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            false,
            overlap);
    }

    Y_UNIT_TEST_QUAD(ReplaceNoColumn, exists, overlap) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_QUAD(ReplaceNoColumn2, exists, overlap) {
        RunTest(
            "REPLACE INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpsertNoColumnNotExists, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];#;[0u]]])",
            R"([[[0u];#]])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpsertNoColumnExists, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpsertNoColumnNotExists2, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpsertNoColumnExists2, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_QUAD(DeleteKey, exists, overlap) {
        RunTest(
            "DELETE FROM `/Root/DataShard` WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_QUAD(DeleteKey2, exists, overlap) {
        RunTest(
            "DELETE FROM `/Root/DataShard` ON SELECT 0u AS Col1;",
            R"([])",
            R"([])",
            exists,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyExists, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col2 = 0u WHERE Col1 = 0u;",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyNotExists, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col2 = 0u WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyExists2, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col3 = 0u WHERE Col1 = 0u;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyNotExists2, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` SET Col3 = 0u WHERE Col1 = 0u;",
            R"([])",
            R"([])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyExists3, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col3;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyNotExists3, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col3;",
            R"([])",
            R"([])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyExists4, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col2;",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true,
            overlap);
    }

    Y_UNIT_TEST_TWIN(UpdateKeyNotExists4, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 0u AS Col2;",
            R"([])",
            R"([])",
            false,
            overlap);
    }

    Y_UNIT_TEST_TWIN(TestUpdateOn, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 1u AS Col2, 0u AS Col3;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true,
            false,
            overlap,
            false);
    }

    Y_UNIT_TEST_TWIN(TestUpdateOnCover, overlap) {
        RunTest(
            "UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 1u AS Col2, 0u AS Col3;",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u];[0u]]])",
            true,
            false,
            overlap,
            true);
    }

}
}
}
