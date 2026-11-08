#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

#include <library/cpp/resource/resource.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYqlTests;

struct TWorkloadQuery {
    ui32 Id;
    TString Text;
};

struct TCompileResult {
    EStatus Status;
    TString Issues;
    std::pair<ui32, ui32> OptimizerCounters;
};

size_t CountParserFallbacks(const TCapturedLog& logs) {
    const auto text = logs.Snapshot();
    constexpr TStringBuf marker = "New YQL parser fallback:";
    size_t count = 0;
    for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) {
        ++count;
    }
    return count;
}

TVector<TCompileResult> CompileWorkload(const TVector<TWorkloadQuery>& queries, EBenchType type,
    bool enableNewParser, bool tpchPerf)
{
    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewYqlParser(enableNewParser);
    config->SetEnableNewRBO(true);
    // A frontend fallback must not require optimizer error fallback.
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableInlineJoinFiltersAfterCBO(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetAllowOlapDataQuery(true);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TCapturedLog logs(true);
    auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
    settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
    TVector<TCompileResult> results;
    {
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        if (tpchPerf) {
            CreateTablesFromPath(tableSession, "schema/tpch.sql", /*useColumnStore=*/true);
        } else {
            CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], /*useColumnStore=*/true);
        }

        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        for (const auto& query : queries) {
            Cerr << "New parser=" << enableNewParser << ", query=" << query.Id << Endl;
            const auto before = GetNewRBOCompileCounters(kikimr);
            if (tpchPerf) {
                // Keep the same API as TPCH_YDB_PERF in the RBO workload suite.
                auto result = tableSession.ExplainDataQuery(query.Text).GetValueSync();
                UNIT_ASSERT_C(!result.IsSuccess() || !result.GetPlan().empty(), "Missing plan for query " << query.Id);
                results.push_back({result.GetStatus(), result.GetIssues().ToString(), {}});
            } else {
                auto result = session.ExecuteQuery(query.Text, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                if (result.IsSuccess()) {
                    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan()
                        && !result.GetStats()->GetPlan()->empty(), "Missing plan for query " << query.Id);
                }
                results.push_back({result.GetStatus(), result.GetIssues().ToString(), {}});
            }
            const auto after = GetNewRBOCompileCounters(kikimr);
            results.back().OptimizerCounters = {after.first - before.first, after.second - before.second};
        }

        if (enableNewParser) {
            // Log records are delivered asynchronously. Check that the feature
            // was exercised, rather than only checking legacy query outcomes.
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (CountParserFallbacks(logs) < queries.size() && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT_C(CountParserFallbacks(logs) >= queries.size(), "Missing frontend fallback records: " << logs.Snapshot());
        }
    }
    if (!enableNewParser) {
        UNIT_ASSERT_VALUES_EQUAL(CountParserFallbacks(logs), 0);
    }
    return results;
}

void CheckWorkload(const TVector<TWorkloadQuery>& queries, EBenchType type, bool tpchPerf = false) {
    const auto baseline = CompileWorkload(queries, type, /*enableNewParser=*/false, tpchPerf);
    const auto fallback = CompileWorkload(queries, type, /*enableNewParser=*/true, tpchPerf);
    size_t successes = 0;
    for (size_t i = 0; i < queries.size(); ++i) {
        const auto context = TStringBuilder() << "Query " << queries[i].Id
            << "\nOriginal pipeline: " << baseline[i].Issues << "\nParser fallback: " << fallback[i].Issues;
        // Include known unsupported workload queries too: their failures must
        // remain failures of the configured optimizer, with the same status.
        UNIT_ASSERT_VALUES_EQUAL_C(fallback[i].Status, baseline[i].Status, context);
        UNIT_ASSERT_VALUES_EQUAL_C(fallback[i].OptimizerCounters, baseline[i].OptimizerCounters, context);
        successes += baseline[i].Status == EStatus::SUCCESS;
    }
    UNIT_ASSERT_C(successes > 0, "No workload query compiled successfully in the baseline");
}

void CheckYqlWorkload(EBenchType type, ui32 first, ui32 last) {
    TVector<TWorkloadQuery> queries;
    for (ui32 id = first; id <= last; ++id) {
        queries.push_back({id, LoadBenchmarkQuery(type, id)});
    }
    CheckWorkload(queries, type);
}

bool HasOptimizerFallbackIssue(const NYdb::NIssue::TIssues& issues) {
    for (const auto& issue : issues) {
        if (issue.GetMessage().find("Compilation with the new RBO failed") != std::string::npos) {
            return true;
        }
    }
    return false;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlParserFallback) {
    Y_UNIT_TEST_TWIN(ResolvesNamesUsingLoadedMetadata, NewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewYqlParser(true);
        config->SetEnableNewRBO(NewRbo);
        config->SetEnableFallbackToYqlOptimizer(false);
        TCapturedLog logs(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
        TKikimrRunner kikimr(settings);
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = tableSession.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/names` (id Int64 NOT NULL, value String, amount Decimal(12,2), PRIMARY KEY(id));").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto valid = session.ExecuteQuery("SELECT n.id, n.value FROM `/Root/names` AS n WHERE n.id > 0 ORDER BY n.id;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(valid.IsSuccess(), valid.GetIssues().ToString());
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (!logs.Snapshot().Contains("New YQL type annotation succeeded") && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(logs.Snapshot().Contains("New YQL name resolution succeeded"), logs.Snapshot());
        UNIT_ASSERT_C(logs.Snapshot().Contains("New YQL type annotation succeeded"), logs.Snapshot());

        auto params = TParamsBuilder().AddParam("$offset").Int64(3).Build().Build();
        auto parameterized = session.ExecuteQuery("SELECT id + $offset, amount FROM `/Root/names` LIMIT 5;",
            NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(parameterized.IsSuccess(), parameterized.GetIssues().ToString());

        auto badType = session.ExecuteQuery("SELECT n.id + n.value FROM `/Root/names` AS n;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!badType.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(badType.GetIssues().ToString(), "New YQL type annotation");

        auto outerJoin = session.ExecuteQuery("SELECT r.id FROM `/Root/names` AS l "
            "LEFT JOIN `/Root/names` AS r ON l.id = r.id;", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(outerJoin.IsSuccess(), outerJoin.GetIssues().ToString());

        auto aggregate = session.ExecuteQuery("SELECT COUNT(*) FROM `/Root/names`;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(aggregate.IsSuccess(), aggregate.GetIssues().ToString());

        auto missing = session.ExecuteQuery("SELECT n.absent FROM `/Root/names` AS n;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!missing.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(missing.GetIssues().ToString(), "New YQL name resolution: Unknown column 'n.absent'");

        auto ambiguous = session.ExecuteQuery("SELECT id FROM `/Root/names` AS l JOIN `/Root/names` AS r ON l.id = r.id;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!ambiguous.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(ambiguous.GetIssues().ToString(), "Ambiguous column");

        // A failed binding must not contaminate the next compilation.
        auto next = session.ExecuteQuery("SELECT id FROM `/Root/names` WHERE id = 7;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(next.IsSuccess(), next.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(SelectAndSyntaxErrors, NewRbo) {
        for (bool enableNewParser : {false, true}) {
            NKikimrConfig::TAppConfig appConfig;
            auto* config = appConfig.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(enableNewParser);
            config->SetEnableNewRBO(NewRbo);
            config->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto tableClient = kikimr.GetTableClient();
            auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
            auto scheme = tableSession.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/t1` (a Int64 NOT NULL, PRIMARY KEY(a));").GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

            auto client = kikimr.GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();
            const auto before = GetNewRBOCompileCounters(kikimr);
            const TVector<TString> queries = {
                "SELECT COUNT(*) FROM `/Root/t1`;",
                "--!ansi_lexer\nSELECT COUNT(*) AS \"count\" FROM `/Root/t1`;",
            };
            for (const auto& query : queries) {
                auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[0u]]");
                UNIT_ASSERT(!HasOptimizerFallbackIssue(result.GetIssues()));
            }
            const auto after = GetNewRBOCompileCounters(kikimr);
            UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
            UNIT_ASSERT_VALUES_EQUAL(after.first - before.first, NewRbo ? queries.size() : 0);

            auto invalid = session.ExecuteQuery("SELECT (", NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!invalid.IsSuccess());
            UNIT_ASSERT(!invalid.GetIssues().Empty());
        }
    }

    Y_UNIT_TEST(TPCH_YQL) {
        CheckYqlWorkload(EBenchType::TPCH, 1, BenchmarkQueryCount[EBenchType::TPCH]);
    }

    Y_UNIT_TEST(TPCDS_YQL_1_25) {
        CheckYqlWorkload(EBenchType::TPCDS, 1, 25);
    }

    Y_UNIT_TEST(TPCDS_YQL_26_50) {
        CheckYqlWorkload(EBenchType::TPCDS, 26, 50);
    }

    Y_UNIT_TEST(TPCDS_YQL_51_75) {
        CheckYqlWorkload(EBenchType::TPCDS, 51, 75);
    }

    Y_UNIT_TEST(TPCDS_YQL_76_99) {
        CheckYqlWorkload(EBenchType::TPCDS, 76, BenchmarkQueryCount[EBenchType::TPCDS]);
    }

    Y_UNIT_TEST(ClickBench_YQL) {
        CheckYqlWorkload(EBenchType::CLICKBENCH, 1, BenchmarkQueryCount[EBenchType::CLICKBENCH]);
    }

    Y_UNIT_TEST(TPCH_YDB_PERF) {
        TVector<TWorkloadQuery> queries;
        const auto constants = NResource::Find("consts.yql");
        for (ui32 id : TpchPerfQueryIds) {
            queries.push_back({id, PrepareTpchPerfQuery(
                NResource::Find(TStringBuilder() << "resfs/file/tpch/queries/yql/q" << id << ".sql"), constants)});
        }
        CheckWorkload(queries, EBenchType::TPCH, /*tpchPerf=*/true);
    }
}

} // namespace NKikimr::NKqp
