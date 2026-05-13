
#include <counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
NKikimrConfig::TAppConfig AppCfgLowComputeLimits(double reasonableTreshold, bool enableSpilling=true, bool limitFileSize=false) {
    NKikimrConfig::TAppConfig appCfg;
    auto* entry = appCfg.MutableLogConfig()->AddEntry();
    appCfg.mutable_tableserviceconfig()->mutable_resourcemanager()->set_verbosememorylimitexception(true);
    entry->SetComponent(NKikimrServices::EServiceKikimr_Name(NKikimrServices::EServiceKikimr::KQP_TASKS_RUNNER));
    entry->SetLevel(NActors::NLog::PRI_DEBUG);

    auto* ts = appCfg.MutableTableServiceConfig();
    ts->SetEnableQueryServiceSpilling(enableSpilling);
    ts->SetEnableSpillingInHashJoinShuffleConnections(false);

    auto* rm = ts->MutableResourceManager();
    rm->SetMkqlLightProgramMemoryLimit(100);
    rm->SetMkqlHeavyProgramMemoryLimit(300);
    rm->SetSpillingPercent(reasonableTreshold);

    auto* spilling = ts->MutableSpillingServiceConfig()->MutableLocalFileConfig();

    spilling->SetRoot("./spilling/");
    if (limitFileSize) {
        spilling->SetMaxFileSize(1);
    }

    return appCfg;
}


Y_UNIT_TEST_SUITE(KqpBlockHashJoin) {
    Y_UNIT_TEST(Spilling) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig = AppCfgLowComputeLimits(0.01);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        int duplicates = 20;
        for (ui32 i = 0; i < 300; ++i) {
            auto str = TString(200000, 'a' + (i / duplicates));
            auto result = queryClient.ExecuteQuery(Sprintf(R"(
                --!syntax_v1
                INSERT INTO `/Root/left_table` (id, data) VALUES (%d, "%s");
                INSERT INTO `/Root/right_table` (id, data) VALUES (%d, "%s");
            )", i, str.data(), i, str.data()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }


        {

            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                        ';
                
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.*
                FROM `left_table` AS L
                INNER JOIN `right_table` AS R
                ON L.data = R.data;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;
            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (UseBlockHashJoin=true): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin when enabled! Actual AST: " << ast);

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            auto expectedRowsCount = 300*duplicates;
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), expectedRowsCount);

            TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
            UNIT_ASSERT(counters.ComputeSpilling.WriteBlobs->Val() > 0);
            UNIT_ASSERT(counters.ComputeSpilling.ReadBlobs->Val() > 0);

        }
    }
    Y_UNIT_TEST_TWIN(BlockHashJoinTest, UseBlockHashJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {

            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = UseBlockHashJoin ? "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n" : "";
            TString select = R"(
                SELECT L.*
                FROM `left_table` AS L
                INNER JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            auto expectedRowsCount = 3;
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), expectedRowsCount);

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (UseBlockHashJoin=" << (UseBlockHashJoin ? "true" : "false") << "): " << ast << Endl;

            if (UseBlockHashJoin) {
                UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                    TStringBuilder() << "AST should contain BlockHashJoin when enabled! Actual AST: " << ast);
            } else {
                UNIT_ASSERT_C(!ast.Contains("BlockHashJoin") && !ast.Contains("DqBlockHashJoin"),
                    TStringBuilder() << "AST should NOT contain BlockHashJoin when disabled! Actual AST: " << ast);
            }
        }
    }

    Y_UNIT_TEST(BlockHashJoinLeftJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3"),
                        (4, "4");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id AS left_id, L.data AS left_data, R.id AS right_id, R.data AS right_data
                FROM `left_table` AS L
                LEFT JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data
                ORDER BY left_id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 4);

            TResultSetParser parser(resultSet);
            
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_id").GetInt32(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_data").GetString(), "1");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_id").GetOptionalInt32().value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_data").GetOptionalString().value(), "1");

            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_id").GetInt32(), 2);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_data").GetString(), "2");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_id").GetOptionalInt32().value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_data").GetOptionalString().value(), "2");

            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_id").GetInt32(), 3);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_data").GetString(), "3");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_id").GetOptionalInt32().value(), 3);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("right_data").GetOptionalString().value(), "3");

            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_id").GetInt32(), 4);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("left_data").GetString(), "4");
            UNIT_ASSERT(!parser.ColumnParser("right_id").GetOptionalInt32().has_value());
            UNIT_ASSERT(!parser.ColumnParser("right_data").GetOptionalString().has_value());

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (LEFT JOIN): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin. Actual AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinLeftSemiJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3"),
                        (4, "4");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id AS id, L.data AS data
                FROM `left_table` AS L
                LEFT SEMI JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data
                ORDER BY id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);

            TResultSetParser parser(resultSet);
            
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetInt32(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("data").GetString(), "1");

            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetInt32(), 2);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("data").GetString(), "2");

            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetInt32(), 3);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("data").GetString(), "3");

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (LEFT SEMI JOIN): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin. Actual AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinLeftOnlyJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3"),
                        (4, "4");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id AS id, L.data AS data
                FROM `left_table` AS L
                LEFT ONLY JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data
                ORDER BY id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

            TResultSetParser parser(resultSet);
            
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetInt32(), 4);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("data").GetString(), "4");

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (LEFT ONLY JOIN): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin. Actual AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinLeftJoinNullCount) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        value Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        value Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, value) VALUES
                        (1, 10),
                        (2, 20),
                        (3, 30),
                        (4, 40),
                        (5, 50);

                    INSERT INTO `/Root/right_table` (id, value) VALUES
                        (1, 100),
                        (3, 300);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id AS left_id, L.value AS left_value, R.id AS right_id, R.value AS right_value
                FROM `left_table` AS L
                LEFT JOIN `right_table` AS R
                ON L.id = R.id
                ORDER BY left_id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 5);

            TResultSetParser parser(resultSet);
            ui32 nullCount = 0;
            ui32 nonNullCount = 0;
            
            while (parser.TryNextRow()) {
                auto rightId = parser.ColumnParser("right_id").GetOptionalInt32();
                auto rightValue = parser.ColumnParser("right_value").GetOptionalInt32();
                
                if (rightId.has_value()) {
                    nonNullCount++;
                    UNIT_ASSERT_C(rightValue.has_value(), "right_value should also be present when right_id is present");
                } else {
                    nullCount++;
                    UNIT_ASSERT_C(!rightValue.has_value(), "right_value should also be NULL when right_id is NULL");
                }
            }
            
            // 3 left rows without matching right rows (ids: 2, 4, 5)
            UNIT_ASSERT_VALUES_EQUAL_C(nullCount, 3, 
                TStringBuilder() << "Expected 3 NULL rows, got " << nullCount);
            // 2 left rows with matching right rows (ids: 1, 3)
            UNIT_ASSERT_VALUES_EQUAL_C(nonNullCount, 2,
                TStringBuilder() << "Expected 2 non-NULL rows, got " << nonNullCount);

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin! Actual AST: " << ast);
        }
    }

    Y_UNIT_TEST(FullJoinFallbackToGraceJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "a"),
                        (2, "b"),
                        (3, "c");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (2, "x"),
                        (3, "y"),
                        (4, "z");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id AS left_id, L.data AS left_data, R.id AS right_id, R.data AS right_data
                FROM `left_table` AS L
                FULL JOIN `right_table` AS R
                ON L.id = R.id
                ORDER BY left_id, right_id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 4);

            TResultSetParser parser(resultSet);

            ui32 leftOnlyCount = 0;
            ui32 rightOnlyCount = 0;
            ui32 matchedCount = 0;

            while (parser.TryNextRow()) {
                auto leftId = parser.ColumnParser("left_id").GetOptionalInt32();
                auto leftData = parser.ColumnParser("left_data").GetOptionalString();
                auto rightId = parser.ColumnParser("right_id").GetOptionalInt32();
                auto rightData = parser.ColumnParser("right_data").GetOptionalString();

                if (leftId.has_value() && !rightId.has_value()) {
                    leftOnlyCount++;
                    UNIT_ASSERT_VALUES_EQUAL(*leftId, 1);
                    UNIT_ASSERT_VALUES_EQUAL(*leftData, "a");
                } else if (!leftId.has_value() && rightId.has_value()) {
                    rightOnlyCount++;
                    UNIT_ASSERT_VALUES_EQUAL(*rightId, 4);
                    UNIT_ASSERT_VALUES_EQUAL(*rightData, "z");
                } else {
                    matchedCount++;
                    UNIT_ASSERT(leftId.has_value() && rightId.has_value());
                    UNIT_ASSERT_VALUES_EQUAL(*leftId, *rightId);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL_C(leftOnlyCount, 1, "Expected 1 left-only row (id=1)");
            UNIT_ASSERT_VALUES_EQUAL_C(rightOnlyCount, 1, "Expected 1 right-only row (id=4)");
            UNIT_ASSERT_VALUES_EQUAL_C(matchedCount, 2, "Expected 2 matched rows (id=2,3)");

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (FULL JOIN with UseBlockHashJoin=true): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("GraceJoin"),
                TStringBuilder() << "FULL JOIN should fall back to GraceJoin. Actual AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("BlockHashJoin") && !ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "FULL JOIN should NOT use BlockHashJoin. Actual AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinWithTypeRemapping) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/table_int64` (
                        id Int64 NOT NULL,
                        value String NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/table_int32` (
                        id Int32 NOT NULL,
                        value String NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/table_int64` (id, value) VALUES
                        (1, "one"),
                        (2, "two"),
                        (3, "three"),
                        (4, "four");

                    INSERT INTO `/Root/table_int32` (id, value) VALUES
                        (1, "uno"),
                        (2, "dos"),
                        (3, "tres");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id, L.value, R.value AS right_value
                FROM `table_int64` AS L
                INNER JOIN `table_int32` AS R
                ON L.id = R.id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL_C(resultSet.RowsCount(), 3, 
                TStringBuilder() << "Expected 3 rows, got " << resultSet.RowsCount());

            TResultSetParser parser(resultSet);
            std::set<int64_t> seenIds;
            while (parser.TryNextRow()) {
                auto id = parser.ColumnParser(0).GetInt64();
                seenIds.insert(id);
                UNIT_ASSERT_C(id >= 1 && id <= 3, TStringBuilder() << "Unexpected id: " << id);
            }
            UNIT_ASSERT_VALUES_EQUAL(seenIds.size(), 3);

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (TypeRemapping): " << ast << Endl;
            UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                TStringBuilder() << "AST should contain BlockHashJoin! Actual AST: " << ast);
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.id, L.value, R.value AS right_value
                FROM `table_int64` AS L
                LEFT JOIN `table_int32` AS R
                ON L.id = R.id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL_C(resultSet.RowsCount(), 4, 
                TStringBuilder() << "Expected 4 rows (all left rows), got " << resultSet.RowsCount());

            TResultSetParser parser(resultSet);
            ui32 matchedRows = 0;
            ui32 unmatchedRows = 0;
            while (parser.TryNextRow()) {
                auto id = parser.ColumnParser(0).GetInt64();
                auto rightValue = parser.ColumnParser(2).GetOptionalString();
                
                if (rightValue.has_value()) {
                    matchedRows++;
                    UNIT_ASSERT_C(id >= 1 && id <= 3, TStringBuilder() << "Matched row has unexpected id: " << id);
                } else {
                    unmatchedRows++;
                    UNIT_ASSERT_VALUES_EQUAL_C(id, 4, "Unmatched row should have id=4");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(matchedRows, 3);
            UNIT_ASSERT_VALUES_EQUAL(unmatchedRows, 1);
        }
    }

    Y_UNIT_TEST(BlockHashJoinColumnPruningInner) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_wide` (
                        id Int32 NOT NULL,
                        name String NOT NULL,
                        extra1 String NOT NULL,
                        extra2 Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_wide` (
                        id Int32 NOT NULL,
                        value String NOT NULL,
                        extra3 String NOT NULL,
                        extra4 Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_wide` (id, name, extra1, extra2) VALUES
                        (1, "alice", "x1", 10),
                        (2, "bob",   "x2", 20),
                        (3, "carol", "x3", 30);

                    INSERT INTO `/Root/right_wide` (id, value, extra3, extra4) VALUES
                        (1, "v1", "y1", 100),
                        (2, "v2", "y2", 200),
                        (4, "v4", "y4", 400);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.name AS name, R.value AS value
                FROM `left_wide` AS L
                INNER JOIN `right_wide` AS R
                ON L.id = R.id
                ORDER BY L.name;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (ColumnPruningInner): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoinCore"),
                TStringBuilder() << "AST should contain BlockHashJoinCore. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("extra1"),
                TStringBuilder() << "Column 'extra1' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("extra2"),
                TStringBuilder() << "Column 'extra2' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("extra3"),
                TStringBuilder() << "Column 'extra3' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("extra4"),
                TStringBuilder() << "Column 'extra4' should be pruned from join. AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinColumnPruningLeftJoinOnlyLeftColumns) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_multi` (
                        id Int32 NOT NULL,
                        col_a String NOT NULL,
                        col_b String NOT NULL,
                        col_c Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_multi` (
                        id Int32 NOT NULL,
                        col_d String NOT NULL,
                        col_e String NOT NULL,
                        col_f Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.col_a AS col_a
                FROM `left_multi` AS L
                LEFT JOIN `right_multi` AS R
                ON L.id = R.id
                ORDER BY L.col_a;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (ColumnPruningLeftJoinOnlyLeftColumns): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoinCore"),
                TStringBuilder() << "AST should contain BlockHashJoinCore. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("col_b"),
                TStringBuilder() << "Column 'col_b' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("col_c"),
                TStringBuilder() << "Column 'col_c' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("col_d"),
                TStringBuilder() << "Column 'col_d' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("col_e"),
                TStringBuilder() << "Column 'col_e' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("col_f"),
                TStringBuilder() << "Column 'col_f' should be pruned from join. AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinColumnPruningWithAggregation) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/orders` (
                        order_id Int32 NOT NULL,
                        customer_id Int32 NOT NULL,
                        amount Int32 NOT NULL,
                        status String NOT NULL,
                        PRIMARY KEY (order_id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/customers` (
                        customer_id Int32 NOT NULL,
                        name String NOT NULL,
                        region String NOT NULL,
                        tier String NOT NULL,
                        PRIMARY KEY (customer_id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(O # 10e12)
                        Bytes(C # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT C.name AS name, SUM(O.amount) AS total
                FROM `orders` AS O
                INNER JOIN `customers` AS C
                ON O.customer_id = C.customer_id
                GROUP BY C.name
                ORDER BY name;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (ColumnPruningWithAggregation): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoinCore"),
                TStringBuilder() << "AST should contain BlockHashJoinCore. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("order_id"),
                TStringBuilder() << "Column 'order_id' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("status"),
                TStringBuilder() << "Column 'status' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("region"),
                TStringBuilder() << "Column 'region' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("tier"),
                TStringBuilder() << "Column 'tier' should be pruned from join. AST: " << ast);
        }
    }

    Y_UNIT_TEST(BlockHashJoinColumnPruningLeftSemiWithExtraColumns) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_extra` (
                        id Int32 NOT NULL,
                        name String NOT NULL,
                        unused1 String NOT NULL,
                        unused2 Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_extra` (
                        id Int32 NOT NULL,
                        value String NOT NULL,
                        unused3 String NOT NULL,
                        unused4 Int32 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n";
            TString select = R"(
                SELECT L.name AS name
                FROM `left_extra` AS L
                LEFT SEMI JOIN `right_extra` AS R
                ON L.id = R.id
                ORDER BY L.name;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (ColumnPruningLeftSemiWithExtraColumns): " << ast << Endl;

            UNIT_ASSERT_C(ast.Contains("BlockHashJoinCore"),
                TStringBuilder() << "AST should contain BlockHashJoinCore. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("unused1"),
                TStringBuilder() << "Column 'unused1' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("unused2"),
                TStringBuilder() << "Column 'unused2' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("unused3"),
                TStringBuilder() << "Column 'unused3' should be pruned from join. AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("unused4"),
                TStringBuilder() << "Column 'unused4' should be pruned from join. AST: " << ast);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
