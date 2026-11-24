#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

TString FormatBenchmarkTraceTitle(TStringBuf suiteName, TStringBuf benchmarkName, ui32 queryId) {
    Y_UNUSED(suiteName);
    const TString benchmark(benchmarkName);
    if (benchmark.StartsWith("TPCDS")) {
        return TStringBuilder() << "TPCDS Q" << queryId;
    }
    if (benchmark.StartsWith("TPCH")) {
        return TStringBuilder() << "TPCH Q" << queryId;
    }
    return TStringBuilder() << benchmark << " Q" << queryId;
}

double TimeQuery(NKikimr::NKqp::TKikimrRunner& kikimr, TString query, int nIterations) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;
}

double TimeQuery(TString schema, TString query, int nIterations) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    session.ExecuteSchemeQuery(schema).GetValueSync();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;
}

void RunTPCHBenchmark(bool columnStore, std::vector<ui32> queries, bool newRbo) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateTablesFromPath(session, "schema/tpch.sql", columnStore);

    if (!queries.size()) {
        for (ui32 i = 1; i <= 22; ++i) {
            queries.push_back(i);
        }
    }

    std::string consts = NResource::Find(TStringBuilder() << "consts.yql");
    std::string tablePrefix = "/Root/";
    for (const auto qId : queries) {
        Cout << "Q " << qId << Endl;
        std::string q = NResource::Find(TStringBuilder() << "resfs/file/tpch/queries/yql/q" << qId << ".sql");
        Replace(q, "{path}", tablePrefix);
        Replace(q, "{% include 'header.sql.jinja' %}", R"(PRAGMA YqlSelect = 'force';)");
        std::regex pattern(R"(\{\{\s*([a-zA-Z0-9_]+)\s*\}\})");
        q = std::regex_replace(q, pattern, "`" + tablePrefix + "$1`");
        q = consts + "\n" + q;
        TScopedRboTraceTitleOverride traceTitle(
            FormatBenchmarkTraceTitle("KqpRboYql", "TPCH_YDB_PERF", qId),
            TString(q.data(), q.size()));
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExplainDataQuery(q).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

void PrintStatus(std::unordered_map<ui32, bool>& queries, std::vector<TString>&& errors) {
    for (const auto &[id, result] : queries) {
        const TString status = result ? "SUCCESS" : "FAIL";
        Cout << "Q#" << id << " " << status << ";" << Endl;
        if (!result) {
            Cout << errors[id - 1] << Endl;
        }
    }
}

static constexpr const char* BenchmarkTraceSuiteName = "KqpRboYql";

static constexpr std::array<const char*, 3> BenchmarkTraceName{"TPCH_YQL", "TPCDS_YQL", "CLICKBENCH_YQL"};

static constexpr std::array<ui32, 3> BenchmarkQueryCount{22, 99, 43};

bool PlanHasJoin(const NJson::TJsonValue& planNode) {
    if (!planNode.IsMap()) {
        return false;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            if (auto opName = op.find("Name"); opName != op.end() && opName->second.GetStringSafe().Contains("Join")) {
                return true;
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (PlanHasJoin(child)) {
                return true;
            }
        }
    }

    return false;
}

void AssertNewRBOCboOptimizedAllTrees(const EBenchType type, const ui32 queryId, const TString& plan) {
    const TString benchmarkName = type == EBenchType::TPCH ? "TPCH" : "TPCDS";
    const TString context = TStringBuilder()
        << benchmarkName << " query " << queryId
        << "\nPlan:\n" << plan;

    NJson::TJsonValue planRoot;
    NJson::ReadJsonTree(plan, &planRoot, true);
    const auto& planRootMap = planRoot.GetMapSafe();
    auto simplifiedPlanIt = planRootMap.find("SimplifiedPlan");
    UNIT_ASSERT_C(simplifiedPlanIt != planRootMap.end(), "Missing SimplifiedPlan. " << context);

    const auto& simplifiedPlan = simplifiedPlanIt->second;
    const auto& simplifiedPlanMap = simplifiedPlan.GetMapSafe();
    auto optimizerStatsIt = simplifiedPlanMap.find("OptimizerStats");
    UNIT_ASSERT_C(optimizerStatsIt != simplifiedPlanMap.end(), "Missing OptimizerStats. " << context);

    const auto& optimizerStats = optimizerStatsIt->second;
    const auto& optimizerStatsMap = optimizerStats.GetMapSafe();
    const auto getStat = [&](const TString& name) {
        auto it = optimizerStatsMap.find(name);
        UNIT_ASSERT_C(it != optimizerStatsMap.end(), "Missing optimizer stat " << name << ". " << context);
        return it->second.GetUIntegerSafe();
    };

    const ui64 total = getStat("CBOTreesTotal");
    const ui64 optimized = getStat("CBOTreesOptimized");
    const TString statsContext = TStringBuilder()
        << "Stats: " << optimizerStats.GetStringRobust()
        << "\n" << context;

    if (PlanHasJoin(simplifiedPlan)) {
        UNIT_ASSERT_C(total > 0, TStringBuilder() << "Expected CBO to see at least one tree. " << statsContext);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(optimized, total, statsContext);
}

void RunPerf_YqlTest(const EBenchType type, const bool columnStore, std::set<ui32>&& queriesStatus, std::set<ui32>&& skipList, const bool newRbo,
                         const bool printStatus = false, const bool compareResults = false, const bool checkNewRBOCbo = false,
                         std::set<ui32>&& queriesWithoutCboCheck = {}) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateTablesFromPath(session, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);

    std::unordered_map<ui32, bool> queriesCurrentStatus;
    std::vector<bool> queriesExpectedStatus;
    std::vector<TString> errors;
    for (ui32 qId = 1, e = BenchmarkQueryCount[type]; qId <= e; ++qId) {
        if (skipList.contains(qId)) {
            queriesCurrentStatus.insert({qId, false});
            queriesExpectedStatus.push_back(false);
            errors.emplace_back("Skipped.");
            continue;
        }

        const auto expectedStatus = queriesStatus.empty() ? true : queriesStatus.contains(qId);
        queriesExpectedStatus.push_back(expectedStatus);
        TString q = GetFullPath(BenchmarkQueryPath[type], ToString(qId) + ".yql");
        const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        const TString round = R"($round = ($x,$y) -> {return $x;};)";

        q = toDecimal + "\n" + toDecimalMax + "\n" + round + "\n" + q;

        Cerr << "Executing benchmark query " << qId << "\n";
        TScopedRboTraceTitleOverride traceTitle(
            FormatBenchmarkTraceTitle(BenchmarkTraceSuiteName, BenchmarkTraceName[type], qId),
            q);

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                          .ExtractValueSync();
        queriesCurrentStatus.insert({qId, result.IsSuccess()});
        errors.emplace_back(result.GetIssues().ToString());
        if (checkNewRBOCbo && result.IsSuccess() && !queriesWithoutCboCheck.contains(qId)) {
            UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan for query: " << qId);
            AssertNewRBOCboOptimizedAllTrees(type, qId, TString{*result.GetStats()->GetPlan()});
        }

    }

    if (printStatus) {
        PrintStatus(queriesCurrentStatus, std::move(errors));
    }

    if (compareResults) {
        for (ui32 i = 0; i < queriesExpectedStatus.size(); ++i) {
            auto status = queriesExpectedStatus[i];
            if (status) {
                UNIT_ASSERT_C(queriesCurrentStatus[i + 1], "Expected success for query: " + ToString(i + 1));
            }
        }
    }
}

std::set<ui32> MakePerf_YqlSingleQuerySkipList(const EBenchType type, const ui32 queryId) {
    std::set<ui32> skipList;
    for (ui32 qId = 1, e = BenchmarkQueryCount[type]; qId <= e; ++qId) {
        if (qId != queryId) {
            skipList.insert(qId);
        }
    }
    return skipList;
}

void RunTPCH_YqlSingleQueryTest(const ui32 queryId, const bool expectedSuccess = true) {
    std::set<ui32> expectedSuccessQueries;
    if (expectedSuccess) {
        expectedSuccessQueries.insert(queryId);
    } else {
        // An empty queriesStatus set means all non-skipped queries are expected to succeed.
        expectedSuccessQueries.insert(BenchmarkQueryCount[EBenchType::TPCH] + 1);
    }

    RunPerf_YqlTest(EBenchType::TPCH, /*columnstore=*/true, std::move(expectedSuccessQueries), MakePerf_YqlSingleQuerySkipList(EBenchType::TPCH, queryId),
                    /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true);
}

void RunPerf_YqlTest(const EBenchType type, ui32 queryId, const bool columnStore, const bool newRbo) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableInlineJoinFiltersAfterCBO(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    auto kikimrSettings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);

    kikimrSettings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::EPriority::PRI_TRACE);
    kikimrSettings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;

    TKikimrRunner kikimr(kikimrSettings);
    
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateTablesFromPath(session, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);

    {
        TString q = GetFullPath(BenchmarkQueryPath[type], ToString(queryId) + ".yql");
        const TString toDecimal =  R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax =  R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        const TString round = R"($round = ($x,$y) -> {return $x;};)";

        q = round + "\n" + toDecimal + "\n" + toDecimalMax + "\n" + q;

        TScopedRboTraceTitleOverride traceTitle(
            FormatBenchmarkTraceTitle(BenchmarkTraceSuiteName, BenchmarkTraceName[type], queryId),
            q);
        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                            .ExtractValueSync();
        Y_ENSURE(result.IsSuccess());
    }
}

void TimePerf_YqlTest(const EBenchType type, ui32 queryId, const bool columnStore, const bool newRbo, const bool useCBO, int nIterations) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    if (!useCBO) {
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
    }
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    auto kikimrSettings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);

    kikimrSettings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
    TKikimrRunner kikimr(kikimrSettings);
    
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateTablesFromPath(session, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);

    {
        TString q = GetFullPath(BenchmarkQueryPath[type], ToString(queryId) + ".yql");
        const TString toDecimal =  R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax =  R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        const TString round = R"($round = ($x,$y) -> {return $x;};)";

        q = round + "\n" + toDecimal + "\n" + toDecimalMax + "\n" + q;

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        clock_t the_time;
        double elapsed_time;
        the_time = clock();

        for (int i=0; i<nIterations; i++) {
            auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                            .ExtractValueSync();
                
            Y_ENSURE(result.IsSuccess());
        }

        TString testName;
        switch(type) {
            case EBenchType::TPCH:
                testName = "tpch";
                break;
            case EBenchType::TPCDS:
                testName = "tpcds";
                break;
            case EBenchType::CLICKBENCH:
                testName = "clickbench";
                break;
            default:
                Y_ENSURE(false, "Unknown benchmark");
        }

        elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
        Cout << testName << "," << queryId << "," << newRbo << "," << elapsed_time / nIterations << "\n";
    }
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(TPCH_YDB_PERF) {
       RunTPCHBenchmark(/*columnstore*/ true, {1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19}, /*new rbo*/ true);
       //RunTPCHBenchmark(/*columnstore*/ true, {1, 6, 14, 19}, /*new rbo*/ false);
    }

    Y_UNIT_TEST(TPCH_9) {
        RunTPCH_YqlSingleQueryTest(9, true);
    }

    Y_UNIT_TEST(TPCDS_14) {
        RunPerf_YqlTest(EBenchType::TPCDS, 14, true, true);
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCH) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        // TPCH
        for (int i=1; i<=22; i++) {
            TimePerf_YqlTest(EBenchType::TPCH, i, true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::TPCH, i, true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCDS) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        TVector<int> tpcdsQueries = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, /*17,*/ 18, 19, 20,
                        21, 22, /*23,*/ 24, 25, 26, /*27,*/ 28, 29, 30, 31, 32, 33, 34, 35, /*36,*/ 37, 38, 39, 40,
                        41, 42, 43, /*44,*/ 45, 46, /*47,*/ 48, 49, 50, /*51,*/ 52, 53, 54, 55, 56, /*57,*/ 58, 59, 60,
                        61, 62, 63, 64, 65, 66, 67, 68, 69, /*70,*/ 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                        81, 82, 83, 84, 85, /*86,*/ 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99};

        for (size_t i=0; i<tpcdsQueries.size(); i++) {
            TimePerf_YqlTest(EBenchType::TPCDS, tpcdsQueries[i], true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::TPCDS, tpcdsQueries[i], true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_CLICKBENCH) {
        const auto perfCompilationEnabled = GetTestParam("ENABLE_PERF_COMPILATION");
        if (perfCompilationEnabled.empty()) {
            return;
        }

        int nIterations = 2;

        TVector<int> clickQueries = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
                                           22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42};
        for (size_t i=0; i<clickQueries.size(); i++) {
            TimePerf_YqlTest(EBenchType::CLICKBENCH, clickQueries[i], true, true, false, nIterations);
            TimePerf_YqlTest(EBenchType::CLICKBENCH, clickQueries[i], true, false, false, nIterations);
        }
    }

    Y_UNIT_TEST(TPCH_YQL) {
        // RunTPCHYqlBenchmark(/*columnstore*/ true, {}, {}, /*new rbo*/ false);
        RunPerf_YqlTest(EBenchType::TPCH, /*columnstore=*/true, {
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
                        /*rbo never finish*/ {}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true,
                        /*queriesWithoutCboCheck=*/{13});
    }

    // Compiled 97 from 99.
    Y_UNIT_TEST(TPCDS_YQL) {
        RunPerf_YqlTest(EBenchType::TPCDS, /*columnstore=*/true,
                        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, /*17,*/ 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                        41, 42, 43, /*44,*/ 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
                        61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                        81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99},
                        /*rbo never finish*/ {}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true, /*checkNewRBOCbo=*/true,
                        // Still explain these queries, but do not require the CBO stats invariant when CBO is explicitly disabled
                        // in the query or until the known gaps are fixed.
                        /*queriesWithoutCboCheck=*/{});
    }

    Y_UNIT_TEST(ClickBench_YQL) {
        // Queries - q19, q29, q40, q43 not supported, because of yql error - not support `GROUP BY ... AS <alias>`.
        RunPerf_YqlTest(EBenchType::CLICKBENCH, /*columnstore=*/true,
                        /*queriesStatus=*/{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
                                           22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42},
                        /*skipList=*/{}, /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true,
                        /*checkNewRBOCbo=*/false, /*queriesWithoutCboCheck=*/{});
    }

    Y_UNIT_TEST(ClickBench_YQL_Single) {
        const ui32 query = 25;
        auto skipList = MakePerf_YqlSingleQuerySkipList(EBenchType::CLICKBENCH, query);
        RunPerf_YqlTest(EBenchType::CLICKBENCH, /*columnstore=*/true,
                        /*queriesStatus=*/{query},
                        /*skipList=*/std::move(skipList), /*new rbo=*/true, /*printStatus=*/false, /*compareResults=*/true,
                        /*checkNewRBOCbo=*/false, /*queriesWithoutCboCheck=*/{});
    }

/*
    Y_UNIT_TEST(PredicatePushdownLeftJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();

        NYdb::TValueBuilder rowsTableT1;
        rowsTableT1.BeginList();
        for (size_t i = 0; i < 2; ++i) {
            rowsTableT1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").String(std::to_string(i) + "_b")
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        NYdb::TValueBuilder rowsTableT2;
        rowsTableT2.BeginList();
        for (size_t i = 0; i < 1; ++i) {
            rowsTableT2.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").String(std::to_string(i) + "_b")
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT2.EndList();

        resultUpsert = db.BulkUpsert("/Root/t2", rowsTableT2.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a, t2.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t1.a = 0;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t2.b = 'some_string';
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` left join `/Root/t2` on t1.a = t2.a where t2.b IS NULL;
            )",
        };

        std::vector<std::string> results = {
            R"([[0;0]])",
            R"([])",
            R"([[1]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(UnionAll) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t3` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t4` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}, {"/Root/t3", 2}, {"/Root/t4", 1}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`
                UNION ALL
                SELECT t3.a FROM `/Root/t3`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1`
                UNION ALL
                SELECT t2.a FROM `/Root/t2`
                UNION ALL
                SELECT t3.a FROM `/Root/t3`
                UNION ALL
                SELECT t4.a FROM `/Root/t4`;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT t1.a FROM `/Root/t1` inner join `/Root/t2` on t1.a = t2.a where t1.a > 1
                UNION ALL
                SELECT t3.a FROM `/Root/t3` where t3.a = 1;
            )",
        };

        std::vector<std::string> results = {
            R"([[0];[1];[2];[3];[0];[1];[2]])",
            R"([[0];[1];[2];[3];[0];[1];[2];[0];[1]])",
            R"([[0];[1];[2];[3];[0];[1];[2];[0];[1];[0]])",
            R"([[2];[1]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(Bench_Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto time = TimeQuery(kikimr, R"(
                --!syntax_pg
                SELECT 1 as "a", 2 as "b";
            )", 10);

        Cout << "Time per query: " << time;

        //UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Bench_Filter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT id as "id2" FROM foo WHERE name = 'some_name';
        )",10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_CrossFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_JoinFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                name	String,
                primary key(id)
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,
                lastname	String,
                primary key(id)
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE f.id = bar.id and name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;
    }

    Y_UNIT_TEST(Bench_10Joins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schema = R"(
CREATE TABLE `/Root/foo_0` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_1` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_2` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_3` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_4` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_5` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_6` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_7` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_8` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );


    CREATE TABLE `/Root/foo_9` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    )";

        auto query = R"(
            --!syntax_pg
     SET TablePathPrefix = "/Root/";

     SELECT foo_0.id as "id2"
     FROM foo_0, foo_1, foo_2, foo_3, foo_4, foo_5, foo_6, foo_7, foo_8, foo_9
     WHERE foo_0.join_id = foo_1.id AND foo_0.join_id = foo_2.id AND foo_0.join_id = foo_3.id AND foo_0.join_id = foo_4.id AND foo_0.join_id = foo_5.id AND
foo_0.join_id = foo_6.id AND foo_0.join_id = foo_7.id AND foo_0.join_id = foo_8.id AND foo_0.join_id = foo_9.id;

    )";

        auto time = TimeQuery(schema, query, 10);

        Cout << "Time per query: " << time;
    }

    */

}

} // namespace NKikimr::NKqp
