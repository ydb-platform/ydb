#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/env.h>

#include <array>
#include <fstream>
#include <regex>
#include <sstream>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

enum class EBenchmark {
    TPCH = 0,
    TPCDS = 1,
};

constexpr std::array<const char*, 2> BenchmarkSchemaPathPrefix{R"(data/)", R"(data/)"};
constexpr std::array<const char*, 2> BenchmarkSchemaPath{R"(schema/tpch.sql)", R"(schema/tpcds.sql)"};
constexpr std::array<const char*, 2> BenchmarkQueryPath{R"(data/yql-tpch/q)", R"(data/yql-tpcds/q)"};
constexpr std::array<ui32, 2> BenchmarkQueryCount{22, 99};

size_t Index(EBenchmark benchmark) {
    return static_cast<size_t>(benchmark);
}

bool NeedOldOptimizerTraceLog() {
    const auto logOutput = TryGetEnv("NEW_RBO_LOG");
    return logOutput.Defined() && !logOutput->empty();
}

TString FormatOldTraceTitle(EBenchmark benchmark, ui32 queryId) {
    switch (benchmark) {
        case EBenchmark::TPCH:
            return TStringBuilder() << "TPCH Q" << queryId << " old";
        case EBenchmark::TPCDS:
            return TStringBuilder() << "TPCDS Q" << queryId << " old";
    }
    return TStringBuilder() << "Query " << queryId << " old";
}

TString GetFullPath(const TString& prefix, const TString& filePath) {
    const TString fullPath = SRC_(prefix + filePath);

    std::ifstream file(fullPath);
    UNIT_ASSERT_C(file.is_open(), "can't open " << fullPath);

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

void CreateTablesFromPath(
    NYdb::NTable::TSession session,
    const TString& pathPrefix,
    const TString& schemaPath,
    bool useColumnStore)
{
    std::string query = GetFullPath(pathPrefix, schemaPath);
    if (useColumnStore) {
        std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
        query = std::regex_replace(
            query,
            pattern,
            "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    }

    auto result = session.ExecuteSchemeQuery(TString(query)).GetValueSync();
    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(result.IsSuccess());
}

NKikimrConfig::TAppConfig MakeOldOptimizerTraceAppConfig() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return appConfig;
}

TString LoadBenchmarkQuery(EBenchmark benchmark, ui32 queryId) {
    const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
    const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
    const TString round = R"($round = ($x,$y) -> {return $x;};)";

    return toDecimal + "\n" +
        toDecimalMax + "\n" +
        round + "\n" +
        GetFullPath(BenchmarkQueryPath[Index(benchmark)], ToString(queryId) + ".yql");
}

void RunOldOptimizerTrace(EBenchmark benchmark) {
    if (!NeedOldOptimizerTraceLog()) {
        Cerr << "Skipping old optimizer trace generation: set NEW_RBO_LOG and run this test explicitly." << Endl;
        return;
    }

    TKikimrRunner kikimr(
        NKqp::TKikimrSettings(MakeOldOptimizerTraceAppConfig()).SetWithSampleTables(false));

    auto db = kikimr.GetTableClient();
    auto tableSession = db.CreateSession().GetValueSync().GetSession();
    CreateTablesFromPath(
        tableSession,
        BenchmarkSchemaPathPrefix[Index(benchmark)],
        BenchmarkSchemaPath[Index(benchmark)],
        /*useColumnStore=*/true);

    auto queryClient = kikimr.GetQueryClient();
    for (ui32 queryId = 1; queryId <= BenchmarkQueryCount[Index(benchmark)]; ++queryId) {
        TString query = LoadBenchmarkQuery(benchmark, queryId);
        const TString traceTitle = FormatOldTraceTitle(benchmark, queryId);

        Cerr << "Generating old optimizer trace for " << traceTitle << Endl;
        TScopedRboTraceTitleOverride traceMetadata(traceTitle, query, MakeRboTraceId());
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        if (!result.IsSuccess()) {
            Cerr << "Old optimizer trace failed for " << traceTitle << ": "
                 << result.GetIssues().ToString() << Endl;
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboOldOptimizerTrace) {
    Y_UNIT_TEST(TPCH) {
        RunOldOptimizerTrace(EBenchmark::TPCH);
    }

    Y_UNIT_TEST(TPCDS) {
        RunOldOptimizerTrace(EBenchmark::TPCDS);
    }
}

} // namespace NKqp
} // namespace NKikimr
