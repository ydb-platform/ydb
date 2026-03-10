#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpStreamIndexes) {
    void RunTest(TString query,
                TString mainResult,
                TString indexResult,
                bool exists,
                bool indexOverlap,
                bool pkOverlap = false,
                bool cover = false,
                bool indexDefault = false) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                Col1 Uint64,
                Col2 Uint64 %s,
                Col3 Uint64,
                INDEX idx GLOBAL ON (Col2 %s) %s,
                PRIMARY KEY (Col1 %s)
            );
        )",
        indexDefault ? "DEFAULT 42" : "",
        indexOverlap ? ", Col1" : "",
        cover ? "COVER (Col3)" : "",
        pkOverlap ? ", Col2" : "");

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

    Y_UNIT_TEST_TWIN(InsertDefaultColumn, overlap) {
        RunTest(
            "INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];[42u];[0u]]])",
            R"([[[0u];[42u]]])",
            false,
            overlap,
            false,
            false,
            true);
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

    Y_UNIT_TEST_TWIN(UpsertDefaultColumnNotExists, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];[42u];[0u]]])",
            R"([[[0u];[42u]]])",
            false,
            overlap,
            false,
            false,
            true);
    }

    Y_UNIT_TEST_TWIN(UpsertDefaultColumnExists, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 0u);",
            R"([[[0u];[1u];[0u]]])",
            R"([[[0u];[1u]]])",
            true,
            overlap,
            false,
            false,
            true);
    }

    Y_UNIT_TEST_TWIN(UpsertDefaultColumnNotExists2, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];#]])",
            R"([[[0u];[0u]]])",
            false,
            overlap,
            false,
            false,
            true);
    }

    Y_UNIT_TEST_TWIN(UpsertDefaultColumnExists2, overlap) {
        RunTest(
            "UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0u);",
            R"([[[0u];[0u];[1u]]])",
            R"([[[0u];[0u]]])",
            true,
            overlap,
            false,
            false,
            true);
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

    Y_UNIT_TEST(SecondaryIsPrimaryPrefix) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Int64, c1 Int32, c2 Utf8, c3 Bool, c4 Int8, c5 Uint64, c6 Uint8, c7 Int32,
                PRIMARY KEY (c0, c1),
                INDEX idx0 GLOBAL UNIQUE SYNC ON (c0),
                INDEX idx1 GLOBAL SYNC ON (c3),
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto it = client.ExecuteQuery(
                R"(INSERT INTO `/Root/DataShard` (c0, c1, c2, c3, c4) VALUES (0, 0, "test", true, 0);)",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto it = client.ExecuteQuery(
                R"(UPSERT INTO `/Root/DataShard` (c0, c1, c2, c3, c4) VALUES (0, 1, "test", true, 0);)",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1 FROM `/Root/DataShard` ORDER BY c0, c1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[0]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1 FROM `/Root/DataShard/idx0/indexImplTable` ORDER BY c0, c1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[0]]])");
        }
    }

    Y_UNIT_TEST(SecondaryNullDelete) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Bool, c1 Uint32, c2 Bool,
                PRIMARY KEY (c0),
                INDEX idx0 GLOBAL SYNC ON (c2) COVER (c1),
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto it = client.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                    (true, 0, true),
                    (false, 0, false),
                    (NULL, 0, true);
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto it = client.ExecuteQuery(
                R"(
                    DELETE FROM `/Root/DataShard` ON (c0) VALUES (true), (false);
                    INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES (false, 0, false);
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_C(it.GetStatus() == NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                $main = SELECT COUNT(*) FROM `/Root/DataShard`;
                $index = SELECT COUNT(*) FROM `/Root/DataShard/idx0/indexImplTable`;
                SELECT $main, $index;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[2u];[2u]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[#;[0u];[%true]];[[%false];[0u];[%false]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard/idx0/indexImplTable` ORDER BY c0, c1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[#;[0u];[%true]];[[%false];[0u];[%false]]])");
        }
    }

    Y_UNIT_TEST(SecondaryUniqueCheckTwoRows) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Bool, c1 Int64, c2 Bool,
                PRIMARY KEY (c0),
                INDEX idx0 GLOBAL UNIQUE SYNC ON (c2),
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto it = client.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                        (false, 0, true),
                        (NULL, NULL, false);
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }


        {
            auto it = client.ExecuteQuery(
                R"(
                    UPSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                        (false, 2817, false),
                        (true, 60403, true);
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[#;#;[%false]];[[%false];[0];[%true]]])");
        }
    }

    Y_UNIT_TEST_TWIN(SecondaryAndReturning, WithIndex) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Int64, c1 Int64, c2 Int64,
                PRIMARY KEY (c0),
                %s
            );
        )", WithIndex ? "INDEX idx0 GLOBAL SYNC ON (c2)," : "");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto it = client.StreamExecuteQuery(
                R"(
                    INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                        (0, 1, 2)
                    RETURNING c0, c1, c2;
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[1];[2]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[1];[2]]])");
        }

        {
            auto it = client.StreamExecuteQuery(
                R"(
                    UPSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                        (0, 10, 20)
                    RETURNING c0, c1, c2;
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[10];[20]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[10];[20]]])");
        }

        {
            auto it = client.StreamExecuteQuery(
                R"(
                    UPDATE `/Root/DataShard`
                    SET c2 = c2 + 10
                    WHERE c0 = 0
                    RETURNING c0, c1, c2;
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[10];[30]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[10];[30]]])");
        }

        {
            auto it = client.StreamExecuteQuery(
                R"(
                    DELETE FROM `/Root/DataShard` ON (c0) VALUES (0)
                    RETURNING c0, c1, c2;
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[10];[30]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([])");
        }
    }

    Y_UNIT_TEST_TWIN(SecondaryAndReturningInteractive, WithIndex) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Int64, c1 Int64, c2 Int64,
                PRIMARY KEY (c0),
                %s
            );
        )", WithIndex ? "INDEX idx0 GLOBAL SYNC ON (c2)," : "");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto session = client.GetSession().GetValueSync().GetSession();

            auto it = session.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES
                        (0, 1, 2)
                    RETURNING c0, c1, c2;
                )",
                NYdb::NQuery::TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = FormatResultSetYson(it.GetResultSet(0));
            CompareYson(output, R"([[[0];[1];[2]]])");

            auto tx = it.GetTransaction();
            UNIT_ASSERT(tx);
            UNIT_ASSERT(tx->IsActive());

            auto commitResult = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT c0, c1, c2 FROM `/Root/DataShard` ORDER BY c0, c1, c2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[0];[1];[2]]])");
        }
    }

    Y_UNIT_TEST(TpccPaymentReturning) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/warehouse` (
                W_ID       Int32          NOT NULL,
                W_YTD      Double,
                W_TAX      Double,
                W_NAME     Utf8,
                W_STREET_1 Utf8,
                W_STREET_2 Utf8,
                W_CITY     Utf8,
                W_STATE    Utf8,
                W_ZIP      Utf8,

                PRIMARY KEY (W_ID)
            );

            CREATE TABLE `/Root/district` (
                D_W_ID      Int32            NOT NULL,
                D_ID        Int32            NOT NULL,
                D_YTD       Double,
                D_TAX       Double,
                D_NEXT_O_ID Int32,
                D_NAME      Utf8,
                D_STREET_1  Utf8,
                D_STREET_2  Utf8,
                D_CITY      Utf8,
                D_STATE     Utf8,
                D_ZIP       Utf8,

                PRIMARY KEY (D_W_ID, D_ID)
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto it = session.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/warehouse` (W_ID, W_YTD, W_TAX, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP) VALUES
                        (1, 1.1, 1.2, "name", "street1", "street2", "city", "state", "zip");
                    INSERT INTO `/Root/district` (D_W_ID, D_ID, D_YTD, D_TAX, D_NEXT_O_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP) VALUES
                        (1, 1, 1.1, 1.2, 1, "name", "street1", "street2", "city", "state", "zip");
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();

            auto params = TParamsBuilder()
                .AddParam("$w_id").Int32(1).Build()
                .AddParam("$payment").Double(100.1).Build()
                .Build();

            auto it = session.ExecuteQuery(
                R"(
                    DECLARE $w_id AS Int32;
                    DECLARE $payment AS Double;

                    UPDATE `/Root/warehouse`
                    SET W_YTD = W_YTD + $payment
                    WHERE W_ID = $w_id
                    RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME;
                )",
                NYdb::NQuery::TTxControl::BeginTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString output = FormatResultSetYson(it.GetResultSet(0));
            CompareYson(output, R"([[["street1"];["street2"];["city"];["state"];["zip"];["name"]]])");

            auto tx = it.GetTransaction();
            UNIT_ASSERT(tx);
            UNIT_ASSERT(tx->IsActive());

            {
                auto params = TParamsBuilder()
                    .AddParam("$d_w_id").Int32(1).Build()
                    .AddParam("$d_id").Int32(1).Build()
                    .AddParam("$payment").Double(100.1).Build()
                    .Build();

                auto it = session.ExecuteQuery(
                R"(
                    DECLARE $d_w_id AS Int32;
                    DECLARE $d_id AS Int32;
                    DECLARE $payment AS Double;

                    UPDATE `/Root/district`
                    SET D_YTD = D_YTD + $payment
                    WHERE D_W_ID = $d_w_id
                    AND D_ID = $d_id
                    RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME;
                )",
                NYdb::NQuery::TTxControl::Tx(*tx), std::move(params)).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString output = FormatResultSetYson(it.GetResultSet(0));
                CompareYson(output, R"([[["street1"];["street2"];["city"];["state"];["zip"];["name"]]])");
            }

            auto commitResult = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT W_ID, W_YTD FROM `/Root/warehouse`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1;[101.2]]])");
        }
    }

    Y_UNIT_TEST_TWIN(SecondaryIndexInsertDuplicates, StreamIndex) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString createQuery = Sprintf(R"(
            CREATE TABLE `/Root/DataShard` (
                c0 Bool, c1 Int64,
                PRIMARY KEY (c0),
                INDEX idx0 GLOBAL SYNC ON (c1),
            );
        )");

        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            NYdb::NQuery::TExecuteQuerySettings execSettings;
            execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
            auto it = client.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/DataShard` (c0, c1) VALUES
                        (false, 0),
                        (false, 1);
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*it.GetStats());
            Cerr << stats.DebugString() << Endl;

            if (StreamIndex) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(2).affected_shards(), 0);
            }
        }

    }
}
}
}
