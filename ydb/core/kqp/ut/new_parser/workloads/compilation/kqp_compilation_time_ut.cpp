#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/host/new_parser/kqp_ast_builder.h>
#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Lexer.h>
#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Parser.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/parser/common/antlr4/depth_limiting_listener.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <set>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYqlTests;

struct TPipeline {
    TStringBuf Name;
    bool NewParser;
    bool NewRbo;
};

constexpr std::array<TPipeline, 3> Pipelines = {{
    {"legacy", false, false},
    {"legacy_new_rbo", false, true},
    {"native", true, true},
}};

// Preserve the original timing workload, including its helper preamble and
// exclusions. Do not simplify queries to make them acceptable to the frontend.
TString QueryText(EBenchType type, ui32 id) {
    return TString("PRAGMA ydb.CostBasedOptimizationLevel = '0';\n")
        + R"($round = ($x,$y) -> {return $x;};)" + "\n"
        + R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)" + "\n"
        + R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)" + "\n"
        + GetFullPath(BenchmarkQueryPath[type], ToString(id) + ".yql");
}

// A successful new-RBO compile alone does not establish native frontend use.
// Check each request's own records, outside the measured interval, and allow
// multiple native roots for the original multi-result TPCDS queries.
void CheckFrontend(const TCapturedLog& logs, size_t start, const TString& traceId, bool native) {
    const TString tag = "TraceId: " + traceId + ", ";
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    TString evidence;
    bool found = false;
    do {
        evidence.clear();
        TVector<TString> lines;
        Split(logs.Snapshot().substr(start), "\n", lines);
        for (const auto& line : lines) {
            if (!line.Contains(tag)) { continue; }
            if (line.Contains("New YQL ") || line.Contains("Legacy YQL graph compilation started")) {
                evidence += line + "\n";
            }
        }
        UNIT_ASSERT_C(!native || (!evidence.Contains(" fallback:")
            && !evidence.Contains("Legacy YQL graph compilation started")), traceId << "\n" << evidence);
        UNIT_ASSERT_C(native || !evidence.Contains("New YQL "), traceId << "\n" << evidence);
        found = evidence.Contains(native ? "New YQL RBO lowering succeeded" : "Legacy YQL graph compilation started");
        if (found) { break; }
        Sleep(TDuration::MilliSeconds(10));
    } while (TInstant::Now() < deadline);
    UNIT_ASSERT_C(found, "Missing frontend evidence for " << traceId << "\n" << evidence);
}

void CheckCboDisabled(const TString& plan, const TString& traceId) {
    NJson::TJsonValue root;
    NJson::ReadJsonTree(plan, &root, true);
    const auto& stats = root.GetMapSafe().at("SimplifiedPlan").GetMapSafe().at("OptimizerStats").GetMapSafe();
    for (const TString name : {"CBOTreesTotal", "CBOTreesOptimized"}) {
        const auto it = stats.find(name);
        UNIT_ASSERT_C(it != stats.end(), "Missing " << name << " for " << traceId);
        UNIT_ASSERT_VALUES_EQUAL_C(it->second.GetUIntegerSafe(), 0, name << " for " << traceId);
    }
}

void SaveTrace(TTestActorRuntime& runtime, const NWilson::TTraceId& parent, const TString& name, bool native) {
    const TString traceId(static_cast<const char*>(parent.GetTraceIdPtr()), parent.GetTraceIdSize());
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    NJson::TJsonValue spans(NJson::JSON_ARRAY);
    do {
        auto promise = NThreading::NewPromise<std::vector<NWilson::TTraceSnapshot::TOtelSpan>>();
        runtime.GetActorSystem(0)->Send(NWilson::MakeWilsonUploaderId(),
            new NWilson::TFakeWilsonUploader::TEvGetSnapshot(promise));
        UNIT_ASSERT_C(promise.GetFuture().Wait(deadline - TInstant::Now()), name);
        spans = NJson::TJsonValue(NJson::JSON_ARRAY);
        bool compiled = false, released = false, replied = false;
        ui32 nameResolutions = 0, metadataRounds = 0;
        for (const auto& span : promise.GetFuture().GetValueSync()) {
            if (span.trace_id() != traceId) { continue; }
            compiled |= span.name() == "Compile query";
            released |= span.name() == "Release compilation state";
            replied |= span.name() == "GrpcRequestProxy";
            nameResolutions += span.name() == "Resolve native names";
            metadataRounds += span.name() == "Await table metadata";
            NJson::TJsonValue item;
            item["name"] = span.name();
            item["span_id"] = HexEncode(span.span_id());
            item["parent_span_id"] = HexEncode(span.parent_span_id());
            item["start_ns"] = span.start_time_unix_nano();
            item["end_ns"] = span.end_time_unix_nano();
            item["status"] = int(span.status().code());
            for (const auto& attribute : span.attributes()) {
                item["attributes"][attribute.key()] = attribute.value().DebugString();
            }
            spans.AppendValue(std::move(item));
        }
        if (compiled && released && replied) {
            if (native) {
                UNIT_ASSERT_VALUES_EQUAL_C(nameResolutions, 1, name);
                UNIT_ASSERT_C(metadataRounds <= 1, name << ": " << metadataRounds << " metadata rounds");
            }
            const auto path = GetOutputPath() / (name + ".trace.json");
            TFileOutput(path).Write(spans.GetStringRobust());
            Cout << "PERF_COMPILATION_TRACE," << name << ',' << path << Endl;
            return;
        }
        Sleep(TDuration::MilliSeconds(10));
    } while (TInstant::Now() < deadline);
    UNIT_FAIL("Incomplete compilation trace: " << name);
}

void Measure(EBenchType type, TStringBuf benchmark, ui32 id, const TPipeline& pipeline, ui32 iterations) {
    const bool tracing = !GetTestParam("PERF_COMPILATION_TRACE").empty();
    NKikimrConfig::TAppConfig appConfig;
    if (tracing) {
        auto* external = appConfig.MutableTracingConfig()->AddExternalThrottling();
        external->SetLevel(15);
        external->SetMaxTracesPerMinute(1'000'000);
        external->SetMaxTracesBurst(1'000'000);
    }
    auto* config = appConfig.MutableTableServiceConfig();
    // Compare the pre-new-RBO baseline, legacy parsing with new RBO, and
    // native parsing with new RBO. All other settings are identical.
    config->SetEnableNewYqlParser(pipeline.NewParser);
    config->SetEnableNewRBO(pipeline.NewRbo);
    config->SetEnableNewRBOPhysicalStagePeephole(false);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetAllowOlapDataQuery(true);
    // The query pragma must override an enabled default in every pipeline.
    // CheckCboDisabled verifies the resulting plan outside the timed interval.
    config->SetDefaultCostBasedOptimizationLevel(4);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    config->SetEnableAstCache(false);
    config->SetCompileQueryCacheSize(0);

    TCapturedLog logs(true);
    auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
    settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    if (tracing) {
        const auto uploader = runtime.Register(new NWilson::TFakeWilsonUploader(), 0);
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploader, node);
        }
    }
    auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    // TPCH/TPCDS measure row storage; ClickBench retains column storage.
    const bool columnStore = type == EBenchType::CLICKBENCH;
    CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], columnStore);
    auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
    const auto query = QueryText(type, id);

    // Warm the parser DFA and compilation machinery once per query/pipeline.
    // Validate the warmup like every measured request, but exclude it from both
    // timing samples and exported traces. Query and AST caches remain disabled.
    for (ui32 iteration = 0; iteration <= iterations; ++iteration) {
        const bool warmup = iteration == 0;
        const TString traceId = TStringBuilder() << "compilation-time-" << benchmark << '-' << id << '-'
            << pipeline.Name << '-' << (warmup ? TString("warmup") : ToString(iteration));
        const auto logStart = logs.Snapshot().size();
        const auto countersBefore = GetNewRBOCompileCounters(kikimr);
        NWilson::TTraceId traceParent;
        auto querySettings = NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            .TraceId(std::string(traceId));
        if (tracing && !warmup) {
            traceParent = NWilson::TTraceId::NewTraceId(15, 4095).Span(15);
            querySettings.TraceParent(traceParent.ToTraceresponseHeader());
        }
        const auto cpuStart = std::clock();
        const auto start = NMonotonic::TMonotonic::Now();
        const auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        const auto wallUs = (NMonotonic::TMonotonic::Now() - start).MicroSeconds();
        const double processCpuUs = double(std::clock() - cpuStart) * 1000000.0 / CLOCKS_PER_SEC;
        UNIT_ASSERT_C(result.IsSuccess(), traceId << ": " << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan() && !result.GetStats()->GetPlan()->empty(), traceId);
        CheckCboDisabled(TString(*result.GetStats()->GetPlan()), traceId);
        // Explain responses omit the SDK compilation-statistics block. With
        // both caches disabled, fresh request-specific frontend records prove
        // that every sample compiled rather than reused a prepared query.
        CheckFrontend(logs, logStart, traceId, pipeline.NewParser);
        const auto countersAfter = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL_C(countersAfter.first - countersBefore.first, pipeline.NewRbo ? 1 : 0, traceId);
        UNIT_ASSERT_VALUES_EQUAL_C(countersAfter.second, countersBefore.second, traceId);
        if (warmup) { continue; }
        if (tracing) { SaveTrace(runtime, traceParent, traceId, pipeline.NewParser); }
        // Keep raw samples for paired summaries. clock() matches the old
        // benchmark's process CPU metric. Explain wall time also includes
        // SDK transport and explain-result postprocessing, not execution.
        Cout << "PERF_COMPILATION," << benchmark << ',' << id << ',' << pipeline.Name << ',' << iteration
            << ',' << wallUs << ',' << processCpuUs << Endl;
    }
}

void RunBenchmark(EBenchType type, TStringBuf benchmark, const TVector<ui32>& queries) {
    if (GetTestParam("ENABLE_PERF_COMPILATION").empty()) { return; }
    ui32 iterations = 2; // Measured repeats after one unmeasured warmup.
    const auto count = GetTestParam("PERF_COMPILATION_ITERATIONS");
    if (!count.empty()) {
        UNIT_ASSERT_C(TryFromString(count, iterations) && iterations > 0 && iterations <= 100,
            "PERF_COMPILATION_ITERATIONS must be between 1 and 100");
    }
    std::set<ui32> selected;
    const auto selection = GetTestParam("PERF_COMPILATION_QUERIES");
    if (!selection.empty()) {
        TVector<TString> ids;
        Split(selection, ",", ids);
        for (const auto& text : ids) {
            ui32 id = 0;
            UNIT_ASSERT_C(TryFromString(text, id) && std::find(queries.begin(), queries.end(), id) != queries.end(), "Invalid query ID: " << text);
            selected.insert(id);
        }
    }
    TVector<TPipeline> pipelines(Pipelines.begin(), Pipelines.end());
    const auto pipelineSelection = GetTestParam("PERF_COMPILATION_PIPELINES");
    if (!pipelineSelection.empty()) {
        pipelines.clear();
        TVector<TString> names;
        Split(pipelineSelection, ",", names);
        std::set<TString> seen;
        for (const auto& name : names) {
            const auto it = std::find_if(Pipelines.begin(), Pipelines.end(),
                [&](const auto& pipeline) { return pipeline.Name == name; });
            UNIT_ASSERT_C(it != Pipelines.end() && seen.insert(name).second, "Invalid or repeated pipeline: " << name);
            pipelines.push_back(*it);
        }
    }
    Cout << "PERF_COMPILATION_CONFIG," << benchmark
        << ",storage=" << (type == EBenchType::CLICKBENCH ? "column" : "row")
        << ",physical_stage_peephole=false,iterations=" << iterations << ",warmup=1,pipelines="
        << (pipelineSelection.empty() ? TString("all") : pipelineSelection) << Endl;
    Cout << "PERF_COMPILATION_HEADER,benchmark,query,pipeline,iteration,explain_wall_us,process_cpu_us" << Endl;
    TString errors;
    for (const auto id : queries) {
        if (!selected.empty() && !selected.contains(id)) { continue; }
        // Rotate which configuration runs first, without concurrent clusters
        // or concurrent benchmark cases contributing to process CPU timings.
        for (size_t offset = 0; offset < pipelines.size(); ++offset) {
            const auto& pipeline = pipelines[(id + offset) % pipelines.size()];
            Cerr << "Compilation timing: " << benchmark << " Q" << id << ' ' << pipeline.Name << Endl;
            try {
                Measure(type, benchmark, id, pipeline, iterations);
            } catch (const std::exception& error) {
                const TString message = TStringBuilder() << benchmark << " Q" << id << ' ' << pipeline.Name << ": " << error.what();
                errors += message + "\n";
                Cerr << message << Endl;
            }
        }
    }
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlCompilationTime) {
    // Run this case alone: clearing the process-wide DFA is only safe without
    // other parsing jobs. No actor runtime or query compilation participates.
    Y_UNIT_TEST(ParserDfaWarmup_TPCDS66) {
        if (GetTestParam("ENABLE_PERF_PARSER").empty()) { return; }
        const auto query = QueryText(EBenchType::TPCDS, 66);
        struct TParserState {
            antlr4::ANTLRInputStream Input;
            NKqpDefaultAntlr4::SQLv1Antlr4Lexer Lexer;
            antlr4::CommonTokenStream Tokens;
            NKqpDefaultAntlr4::SQLv1Antlr4Parser Parser;

            explicit TParserState(const TString& text)
                : Input(text), Lexer(&Input), Tokens(&Lexer), Parser(&Tokens)
            {}
        };
        size_t previousStates = 0;
        Cout << "PERF_PARSER_HEADER,iteration,reset_dfa,dfa_before,dfa_after,initialize_us,parse_us,build_ast_us,release_cst_us" << Endl;
        for (ui32 iteration = 1; iteration <= 12; ++iteration) {
            auto start = NMonotonic::TMonotonic::Now();
            auto state = std::make_unique<TParserState>(query);
            const auto initializeUs = (NMonotonic::TMonotonic::Now() - start).MicroSeconds();
            auto* simulator = state->Parser.getInterpreter<antlr4::atn::ParserATNSimulator>();
            const auto countStates = [&] {
                size_t count = 0;
                for (const auto& dfa : simulator->decisionToDFA) { count += dfa.states.size(); }
                return count;
            };
            // A second cold/warm pair distinguishes DFA reuse from incidental
            // allocator, instruction-cache or first-process initialization costs.
            const bool reset = iteration == 7;
            if (reset) {
                simulator->clearDFA();
                state->Lexer.getInterpreter<antlr4::atn::LexerATNSimulator>()->clearDFA();
            }
            const auto before = countStates();
            if (iteration > 1 && !reset) { UNIT_ASSERT_VALUES_EQUAL(before, previousStates); }
            if (reset) { UNIT_ASSERT_VALUES_EQUAL(before, 0); }
            NAntlrAST::TDepthLimitingListener depthLimiter(4096);
            state->Lexer.removeErrorListeners();
            state->Parser.removeErrorListeners();
            state->Parser.addParseListener(&depthLimiter);
            state->Parser.setBuildParseTree(true);
            start = NMonotonic::TMonotonic::Now();
            auto* root = state->Parser.sql_query();
            const auto parseUs = (NMonotonic::TMonotonic::Now() - start).MicroSeconds();
            state->Parser.removeParseListener(&depthLimiter);
            UNIT_ASSERT_VALUES_EQUAL(state->Parser.getNumberOfSyntaxErrors(), 0);
            previousStates = countStates();
            UNIT_ASSERT(previousStates > 0);
            start = NMonotonic::TMonotonic::Now();
            auto ast = BuildNewYqlAst(*root, false, {});
            const auto buildUs = (NMonotonic::TMonotonic::Now() - start).MicroSeconds();
            UNIT_ASSERT(ast);
            start = NMonotonic::TMonotonic::Now();
            state.reset();
            const auto releaseUs = (NMonotonic::TMonotonic::Now() - start).MicroSeconds();
            Cout << "PERF_PARSER," << iteration << ',' << reset << ',' << before << ',' << previousStates
                << ',' << initializeUs << ',' << parseUs << ',' << buildUs << ',' << releaseUs << Endl;
        }
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCH) {
        TVector<ui32> queries;
        for (ui32 id = 1; id <= 22; ++id) { queries.push_back(id); }
        RunBenchmark(EBenchType::TPCH, "tpch", queries);
    }

    Y_UNIT_TEST(CompilationTimeBench_TPCDS) {
        RunBenchmark(EBenchType::TPCDS, "tpcds", {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20,
            21, 22, 24, 25, 26, 28, 29, 30, 31, 32, 33, 34, 35, 37, 38, 39, 40,
            41, 42, 43, 45, 46, 48, 49, 50, 52, 53, 54, 55, 56, 58, 59, 60,
            61, 62, 63, 64, 65, 66, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
            81, 82, 83, 84, 85, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99});
    }

    Y_UNIT_TEST(CompilationTimeBench_CLICKBENCH) {
        RunBenchmark(EBenchType::CLICKBENCH, "clickbench", {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42});
    }
}

} // namespace NKikimr::NKqp
