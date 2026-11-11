#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/env.h>

#include <util/stream/file.h>
#include <util/system/shellcommand.h>

namespace NKikimr::NKqp {
namespace {

void RunTpchCli(const TString& endpoint, const TString& database, const TVector<TString>& arguments) {
    TShellCommand command(BinaryPath("ydb/apps/ydb/ydb"),
        TShellCommandOptions().SetUseShell(false).SetCloseAllFdsOnExec(true));
    // The statistics fixture exposes a root endpoint, without tenant discovery endpoints.
    command << "--endpoint" << "grpc://" + endpoint << "--database" << database << "--no-discovery"
        << "workload" << "tpch" << "--path" << "tpch";
    for (const auto& argument : arguments) {
        command << argument;
    }
    Cerr << "Running " << command.GetQuotedCommand() << Endl;
    command.Run().Wait();
    UNIT_ASSERT_C(command.GetExitCode() == 0,
        command.GetQuotedCommand() << "\nstdout:\n" << command.GetOutput() << "\nstderr:\n" << command.GetError());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboStatistics) {
    Y_UNIT_TEST(ExplainTpchFilterAfterAnalyze) {
        NStat::TTestEnv env(1, 1, true, [](Tests::TServerSettings& settings) {
            auto* config = settings.AppConfig->MutableTableServiceConfig();
            config->SetEnableNewRBO(true);
            config->SetEnableFallbackToYqlOptimizer(false);
            config->SetDefaultCostBasedOptimizationLevel(4);
        });
        const TString database = NStat::CreateDatabase(env, "Database");
        RunTpchCli(env.GetEndpoint(), database, {"init", "--store=row", "--scale=0.1"});
        RunTpchCli(env.GetEndpoint(), database,
            {"import", "--upload-threads=2", "--max-in-flight=2", "generator", "--scale=0.1"});

        NYdb::TDriver driver(NYdb::TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDatabase(database)
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        NYdb::NTable::TTableClient tableClient(driver);
        auto tableSessionResult = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(tableSessionResult.IsSuccess(), tableSessionResult.GetIssues().ToString());
        auto tableSession = tableSessionResult.GetSession();
        for (const TString table : {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}) {
            Cerr << "Analyzing " << table << Endl;
            const auto analyzed = tableSession.ExecuteSchemeQuery(
                "ANALYZE `" + database + "/tpch/" + table + "`;").GetValueSync();
            UNIT_ASSERT_C(analyzed.IsSuccess(), table << ": " << analyzed.GetIssues().ToString());
        }

        NYdb::NQuery::TQueryClient queryClient(driver);
        auto sessionResult = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto countRows = [&](const TString& query) {
            const auto result = session.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NYdb::TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser(0).GetUint64();
        };
        const auto actualRows = countRows("SELECT COUNT(*) FROM `tpch/customer`;");
        const auto actualFilteredRows = countRows(R"(
            SELECT COUNT(*) FROM `tpch/customer` WHERE c_mktsegment = 'HOUSEHOLD';
        )");
        UNIT_ASSERT_VALUES_EQUAL(actualRows, 15000); // TPCH customer at scale 0.1.
        UNIT_ASSERT_GT(actualFilteredRows, 0);
        UNIT_ASSERT_LT(actualFilteredRows, actualRows);

        auto& runtime = *env.GetServer().GetRuntime();
        const auto customerId = NStat::ResolvePathId(runtime, database + "/tpch/customer");
        // Wait for live shard statistics to reach the query node before compiling Explain.
        NStat::WaitForRowCount(runtime, 0, customerId, actualRows);
        const auto columnStats = NStat::GetStatistics(runtime, customerId,
            NStat::EStatType::SIMPLE_COLUMN, {5}); // c_mktsegment in the CLI TPCH schema.
        UNIT_ASSERT_VALUES_EQUAL(columnStats.size(), 1);
        UNIT_ASSERT(columnStats[0].Success && columnStats[0].SimpleColumn.Data);
        UNIT_ASSERT_VALUES_EQUAL(columnStats[0].SimpleColumn.Data->GetCount(), actualRows);
        UNIT_ASSERT_VALUES_EQUAL(columnStats[0].SimpleColumn.Data->GetCountDistinct(), 5);

        const auto sketches = NStat::GetStatistics(runtime, customerId,
            NStat::EStatType::COUNT_MIN_SKETCH, {5});
        UNIT_ASSERT_VALUES_EQUAL(sketches.size(), 1);
        UNIT_ASSERT(sketches[0].Success && sketches[0].CountMinSketch.CountMin);
        const TString segment = "HOUSEHOLD";
        UNIT_ASSERT_VALUES_EQUAL(
            sketches[0].CountMinSketch.CountMin->Probe(segment.data(), segment.size()), actualFilteredRows);

        const auto result = session.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT c_custkey, c_name
            FROM `tpch/customer`
            WHERE c_mktsegment = 'HOUSEHOLD';
        )", NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats() && result.GetStats()->GetPlan());
        const TString plan(*result.GetStats()->GetPlan());
        GetOutputPath().MkDirs();
        const auto outputPath = GetOutputPath().Child("tpch_filter.explain.json");
        TOFStream output(outputPath);
        output << plan << Endl;
        output.Finish();
        Cerr << "Actual rows: " << actualRows << ", HOUSEHOLD rows: " << actualFilteredRows << Endl
            << "Explain JSON: " << outputPath << Endl << plan << Endl;

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(plan, &json, true));
        const auto& simplified = json.GetMapSafe().at("SimplifiedPlan");
        const auto filter = FindPlanNodeByKv(simplified, "Name", "Filter");
        const auto read = FindPlanNodeByKv(simplified, "Table", "customer");
        UNIT_ASSERT_C(filter.IsDefined(), plan);
        UNIT_ASSERT_C(read.IsDefined(), plan);
        UNIT_ASSERT_VALUES_EQUAL_C(read.GetMapSafe().at("E-Rows").GetDoubleSafe(), actualRows, plan);
        UNIT_ASSERT_DOUBLES_EQUAL_C(filter.GetMapSafe().at("E-Rows").GetDoubleSafe(), actualFilteredRows, 0.01, plan);
    }
}

} // namespace NKikimr::NKqp
