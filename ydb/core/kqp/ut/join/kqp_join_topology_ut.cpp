#include "kqp_join_topology_generator.h"

#include <library/cpp/testing/common/env.h>
#include <util/generic/array_size.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_arg_parser.h>
#include <ydb/core/kqp/ut/common/kqp_benches.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <cstdint>
#include <exception>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <string>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpJoinTopology) {

    std::optional<TString> ExplainQuery(NYdb::NQuery::TSession session, const std::string& query) {
        auto explainRes = session.ExecuteQuery(query,
          NYdb::NQuery::TTxControl::NoTx(),
          NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        if (explainRes.GetStatus() == EStatus::TIMEOUT) {
            return std::nullopt;
        }

        explainRes.GetIssues().PrintTo(Cout);
        if (explainRes.GetStatus() != EStatus::SUCCESS) {
            throw std::runtime_error("Couldn't execute query!");
        }

        return *explainRes.GetStats()->GetPlan();
    }

    bool ExecuteQuery(NYdb::NQuery::TSession session, std::string query) {
        auto execRes = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

        if (execRes.GetStatus() == EStatus::TIMEOUT) {
            return false;
        }

        execRes.GetIssues().PrintTo(Cout);
        UNIT_ASSERT(execRes.IsSuccess());

        return true;
    }

    void JustPrintPlan(const TString& plan) {
        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
            NYdb::NConsoleClient::EDataFormat::PrettyTable,
            /*analyzeMode=*/true, Cout, /*maxWidth=*/0);

        queryPlanPrinter.Print(plan);
    }

    std::string ConfigureQuery(const std::string& query, bool enableShuffleElimination = false, unsigned optLevel = 2) {
        std::string queryWithShuffleElimination = "PRAGMA ydb.OptShuffleElimination=\"";
        queryWithShuffleElimination += enableShuffleElimination ? "true" : "false";
        queryWithShuffleElimination += "\";\n";
        queryWithShuffleElimination += "PRAGMA ydb.MaxDPHypDPTableSize='4294967295';\n";
        queryWithShuffleElimination += "PRAGMA ydb.ShuffleEliminationJoinNumCutoff='" + std::to_string(UINT32_MAX) + "';\n";
        queryWithShuffleElimination += "PRAGMA ydb.CostBasedOptimizationLevel=\"" + std::to_string(optLevel) + "\";\n";
        queryWithShuffleElimination += query;

        return queryWithShuffleElimination;
    }

    std::optional<TStatistics> BenchmarkExplain(TBenchmarkConfig config, NYdb::NQuery::TSession session, const TString& query) {
        std::optional<TString> savedPlan = std::nullopt;
        std::optional<TStatistics> stats = Benchmark(config, [&]() -> bool {
            auto plan = ExplainQuery(session, query);
            if (!savedPlan) {
                savedPlan = plan;
            }

            return !!plan;
        });

        if (!stats) {
            Cout << "-------------------------------- TIMED OUT -------------------------------\n";
            return std::nullopt;
        }

        Y_ASSERT(savedPlan);
        JustPrintPlan(*savedPlan);
        Cout << "--------------------------------------------------------------------------\n";

        DumpTimeStatistics(stats->ComputeStatistics(), Cout);

        return stats;
    }

    std::optional<std::map<std::string, TStatistics>> BenchmarkShuffleElimination(TBenchmarkConfig config, NYdb::NQuery::TSession session, std::string resultType, const TString& query) {
        std::map<std::string, TStatistics> results;

        std::optional<TStatistics> withoutCBO;
        if (resultType.contains("0")) {
            Cout << "--------------------------------- W/O CBO --------------------------------\n";

            withoutCBO = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/0));
            if (!withoutCBO) {
                return std::nullopt;
            }

            results.emplace("0", *withoutCBO);
            if (withoutCBO->GetMax() > config.SingleRunTimeout || resultType == "0") {
                return results;
            }
        }

        std::optional<TStatistics> withoutShuffleElimination;
        if (resultType.contains("CBO")) {
            Cout << "--------------------------------- CBO-SE ---------------------------------\n";
            withoutShuffleElimination = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/2));

            if (resultType.contains("0")) {
                results.emplace("CBO-0", (*withoutShuffleElimination - *withoutCBO).Filter([](double value) { return value > 0; }));
            }

            if (!withoutShuffleElimination) {
                return std::nullopt;
            }

            results.emplace("CBO", *withoutShuffleElimination);
            if (withoutShuffleElimination->GetMax() > config.SingleRunTimeout || resultType == "CBO" || resultType == "CBO-0") {
                return results;
            }
        }

        std::optional<TStatistics> withShuffleElimination;
        if (resultType.contains("SE")) {
            Cout << "--------------------------------- CBO+SE ---------------------------------\n";
            withShuffleElimination = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/true, /*optLevel=*/2));

            if (resultType.contains("0")) {
                results.emplace("SE-0", (*withShuffleElimination - *withoutCBO).Filter([](double value) { return value > 0; }));
            }

            if (!withShuffleElimination) {
                return std::nullopt;
            }

            results.emplace("SE", *withShuffleElimination);
            if (withShuffleElimination->GetMax() > config.SingleRunTimeout || resultType == "SE" || resultType == "SE-0") {
                return results;
            }
        }

        Cout << "--------------------------------------------------------------------------\n";

        results.emplace("SE-div-CBO", *withShuffleElimination / *withoutShuffleElimination);
        if (resultType.contains("0")) {
            auto& adjustedWithTime = results.at("SE-0");
            auto& adjustedWithoutTime = results.at("CBO-0");

            if (adjustedWithTime.ComputeStatistics().Median > adjustedWithoutTime.ComputeStatistics().Median) {
                auto adjustedRatio = adjustedWithTime / adjustedWithoutTime;
                results.emplace("SE-0-div-CBO-0", adjustedRatio);
            }
        }

        return results;
    }

    std::unique_ptr<TKikimrRunner> GetCBOTestsYDB(TString stats, TDuration compilationTimeout) {
        TVector<NKikimrKqp::TKqpSetting> settings;

        Y_ASSERT(!stats.empty());

        NKikimrKqp::TKqpSetting setting;
        setting.SetName("OptOverrideStatistics");
        setting.SetValue(stats);
        settings.push_back(setting);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
        appConfig.MutableTableServiceConfig()->SetEnableOrderOptimizaionFSM(true);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(compilationTimeout.MilliSeconds());

        TKikimrSettings serverSettings(appConfig);
        serverSettings.SetWithSampleTables(false);
        serverSettings.SetKqpSettings(settings);

        return std::make_unique<TKikimrRunner>(serverSettings);
    }

    std::optional<std::map<std::string, TStatistics>>
    BenchmarkShuffleEliminationOnTopology(TBenchmarkConfig config, NYdb::NQuery::TSession session, std::string resultType, TRelationGraph graph) {
        Cout << "================================= CREATE =================================\n";
        graph.DumpGraph(Cout);

        Cout << "================================= REORDER ================================\n";
        graph.ReorderDFS();
        graph.DumpGraph(Cout);

        Cout << "================================= PREPARE ================================\n";
        auto creationQuery = graph.GetSchema().MakeCreateQuery();
        Cout << creationQuery;
        if (!ExecuteQuery(session, creationQuery)) {
            return std::nullopt;
        }

        Y_SCOPE_EXIT(&) {
            Cout << "================================= FINALIZE ===============================\n";
            auto deletionQuery = graph.GetSchema().MakeDropQuery();
            Cout << deletionQuery;

            bool deletionSucceeded = ExecuteQuery(session, deletionQuery);
            UNIT_ASSERT_C(deletionSucceeded, "Table deletion timeouted, can't proceed!");
            Cout << "==========================================================================\n";
        };

        Cout << "================================= BENCHMARK ==============================\n";
        TString query = graph.MakeQuery();
        Cout << query;

        return BenchmarkShuffleElimination(config, session, resultType, query);
    }

    template <typename TValue>
    void OverrideWithArg(std::string key, TArgs args, auto& value) {
        if (args.HasArg(key)) {
            value = args.GetArg<TValue>(key).GetValue();
        }
    }

    void OverrideRepeatedTestConfig(std::string prefix, TArgs args, TRepeatedTestConfig& config) {
        OverrideWithArg<uint64_t>(prefix + ".MinRepeats", args, config.MinRepeats);
        OverrideWithArg<uint64_t>(prefix + ".MaxRepeats", args, config.MaxRepeats);
        OverrideWithArg<std::chrono::nanoseconds>(prefix + ".Timeout", args, config.Timeout);
    }

    TBenchmarkConfig GetBenchmarkConfig(TArgs args, std::string prefix = "config") {
        TBenchmarkConfig config = /*default=*/{
            .Warmup = {
                .MinRepeats = 1,
                .MaxRepeats = 5,
                .Timeout = 1'000'000'000,
            },

            .Bench = {
                .MinRepeats = 10,
                .MaxRepeats = 30,
                .Timeout = 10'000'000'000,
            },

            .SingleRunTimeout = 20'000'000'000,
            .MADThreshold = 0.05
        };

        OverrideRepeatedTestConfig(prefix + ".Warmup", args, config.Warmup);
        OverrideRepeatedTestConfig(prefix + ".Bench", args, config.Bench);
        OverrideWithArg<double>(prefix + ".MADThreshold", args, config.MADThreshold);
        OverrideWithArg<std::chrono::nanoseconds>(prefix + ".SingleRunTimeout", args, config.SingleRunTimeout);

        return config;
    }

    void DumpBenchmarkConfig(IOutputStream& os, TBenchmarkConfig config) {
        os << "config = {\n";
        os << "    .Warmup = {\n";
        os << "         .MinRepeats = " << config.Warmup.MinRepeats << ",\n";
        os << "         .MaxRepeats = " << config.Warmup.MaxRepeats << ",\n";
        os << "         .Timeout = " << TimeFormatter::Format(config.Warmup.Timeout) << "\n";
        os << "    },\n\n";
        os << "    .Bench = {\n";
        os << "        .MinRepeats = " << config.Bench.MinRepeats << ",\n";
        os << "        .MaxRepeats = " << config.Bench.MaxRepeats << ",\n";
        os << "        .Timeout = " << TimeFormatter::Format(config.Bench.Timeout) << "\n";
        os << "    },\n\n";
        os << "    .SingleRunTimeout = " << TimeFormatter::Format(config.SingleRunTimeout) << ",\n";
        os << "    .MADThreshold = " << config.MADThreshold << "\n";
        os << "}\n";
    }

    TPitmanYorConfig GetPitmanYorConfig(TArgs args) {
        return TPitmanYorConfig{
            .Alpha = args.GetArgOrDefault<double>("alpha", "0.5").GetValue(),
            .Theta = args.GetArgOrDefault<double>("theta", "1.0").GetValue()
        };
    }

    struct TBenchState {
        ui32 Seed;
        ui32 TopologyCounter;
        ui32 MCMCCounter;
        ui32 KeyCounter;

        std::string toHex() const {
            std::stringstream ss;
            ss << std::hex << std::setfill('0');
            ss << std::setw(8) << Seed
               << std::setw(8) << TopologyCounter
               << std::setw(8) << MCMCCounter
               << std::setw(8) << KeyCounter;
            return ss.str();
        }

        static TBenchState fromHex(const std::string& hex) {
            TBenchState state;
            ui32* parts[] = {&state.Seed, &state.TopologyCounter, &state.MCMCCounter, &state.KeyCounter};
            for (ui32 i = 0; i < Y_ARRAY_SIZE(parts); ++ i) {
                *parts[i] = std::stoul(hex.substr(i * 8, 8), nullptr, 16);
            }

            return state;
        }
    };

    struct TTestContext {
        std::unique_ptr<TKikimrRunner> Runner;
        NYdb::NQuery::TQueryClient QueryClient;
        NYdb::NQuery::TSession Session;

        std::optional<TBenchState> State;
        TRNG RNG;

        std::string OutputDir;
        std::map<std::string, TUnbufferedFileOutput> Streams = {};
    };

    TTestContext CreateTestContext(TArgs args, std::string outputDir = "") {
        std::random_device randomDevice;
        TRNG rng(randomDevice() % UINT32_MAX);

        std::optional<TBenchState> state;
        if (args.HasArg("state")) {
            state = TBenchState::fromHex(args.GetString("state"));
            rng.seed(state->Seed);
        }

        auto numTablesRanged = args.GetArg<uint64_t>("N");

        TSchema fullSchema = TSchema::MakeWithEnoughColumns(numTablesRanged.GetLast());
        TString stats = TSchemaStats::MakeRandom(rng, fullSchema, 7, 10).ToJSON();

        auto kikimr = GetCBOTestsYDB(stats, TDuration::Seconds(10));
        auto db = kikimr->GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        return {std::move(kikimr), std::move(db), std::move(session), state, std::move(rng), outputDir};
    }

    std::string WriteGraph(TTestContext& ctx, ui32 graphID, const TRelationGraph& graph) {
        if (ctx.OutputDir.empty()) {
            return "";
        }

        std::string graphDir = ctx.OutputDir + "/graphs";
        if (!std::filesystem::exists(graphDir)) {
            std::filesystem::create_directories(graphDir);
        }

        std::string graphName = TLexicographicalNameGenerator::getName(graphID);

        TString filename = graphDir + "/" + graphName + ".dot";
        auto os = TUnbufferedFileOutput(filename);
        graph.DumpGraph(os);

        return graphName;
    }

    void WriteAllStats(TTestContext& ctx, const std::string& prefix,
                       const std::string& header, const std::string& params,
                       const std::map<std::string, TStatistics>& stats) {
        if (ctx.OutputDir.empty()) {
            return;
        }

        if (!std::filesystem::exists(ctx.OutputDir)) {
            std::filesystem::create_directories(ctx.OutputDir);
        }

        for (const auto& [key, stat] : stats) {
            std::string name = prefix + key;

            if (!ctx.Streams.contains(name)) {
                auto filename = TString(ctx.OutputDir + "/" + name + ".csv");
                auto& os = ctx.Streams.emplace(name, TUnbufferedFileOutput(filename)).first->second;
                os << header << "\n";
            }

            auto& os = ctx.Streams.find(name)->second;

            os << params << ",";
            stat.ComputeStatistics().ToCSV(os);
            os << "\n";
        }
    }

    void AccumulateAllStats(std::map<std::string, TStatistics>& cummulative,
                            const std::map<std::string, TStatistics>& stats) {
        for (auto& [key, stat] : stats) {
            if (!cummulative.contains(key)) {
                cummulative.emplace(key, stat);
            }

            cummulative.find(key)->second.Merge(stat);
        }
    }

    using TTopologyFunction = TRelationGraph (*)(TRNG& rng, ui32 n, double mu, double sigma);
    template <auto TTrivialTopologyGenerator>
    TTopologyFunction GetTrivialTopology() {
        return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma) {
            return TTrivialTopologyGenerator(n);
        };
    }

    TTopologyFunction GetTopology(std::string topologyName) {
        if (topologyName == "star") {
            return GetTrivialTopology<GenerateStar>();
        }

        if (topologyName == "path") {
            return GetTrivialTopology<GeneratePath>();
        }

        if (topologyName == "clique") {
            return GetTrivialTopology<GenerateClique>();
        }

        if (topologyName == "random-tree") {
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma) {
                return GenerateRandomTree(rng, n);
            };
        }

        if (topologyName == "mcmc") {
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma) {
                Cout << "================================= METRICS ================================\n";
                auto sampledDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                Cout << "sampled degrees: " << JoinSeq(", ", sampledDegrees) << "\n";

                auto graphicDegrees = MakeGraphicConnected(sampledDegrees);
                Cout << "graphic degrees: " << JoinSeq(", ", graphicDegrees) << "\n";

                auto initialGraph = ConstructGraphHavelHakimi(graphicDegrees);
                return initialGraph;
            };
        }

        if (topologyName == "chung-lu") {
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma) {
                Cout << "================================= METRICS ================================\n";
                auto initialDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                Cout << "initial degrees: " << JoinSeq(", ", initialDegrees) << "\n";

                auto initialGraph = GenerateRandomChungLuGraph(rng, initialDegrees);
                return initialGraph;
            };
        }

        throw std::runtime_error("Unknown topology: '" + topologyName + "'");
    }

    void RunBenches(TTestContext& ctx, TBenchmarkConfig config, TArgs args) {
        std::string resultType = args.GetStringOrDefault("result", "SE");

        ui64 topologyGenerationRepeats = args.GetArgOrDefault<uint64_t>("gen-n", "1").GetValue();
        ui64 mcmcRepeats = args.GetArgOrDefault<uint64_t>("mcmc-n", "1").GetValue();
        ui64 equiJoinKeysGenerationRepeats = args.GetArgOrDefault<uint64_t>("keys-n", "1").GetValue();

        std::string topologyName = args.GetStringOrDefault("type", "star");
        auto generateTopology = GetTopology(topologyName);

        std::string headerAggregate = "idx,N,alpha,theta,sigma,mu," + TComputedStatistics::GetCSVHeader();
        std::string header = "idx,state,graph_name,aggregate_" + headerAggregate;

        ui32 idx = 0;
        ui32 aggregateIdx = 0;

        for (double alpha : args.GetArgOrDefault<double>("alpha", "0.5")) {
            for (double theta : args.GetArgOrDefault<double>("theta", "1.0")) {
                for (double sigma : args.GetArgOrDefault<double>("sigma", "0.5")) {
                    for (double mu : args.GetArgOrDefault<double>("mu", "1.0")) {
                        for (ui64 n : args.GetArg<ui64>("N")) {
                            std::stringstream commonParams;
                            commonParams << (aggregateIdx ++)
                                         << "," << n
                                         << "," << alpha << "," << theta
                                         << "," << sigma << "," << mu;

                            std::map<std::string, TStatistics> aggregate;
                            for (ui64 i = 0; i < topologyGenerationRepeats; ++ i) {
                                Cout << "\n\n\n";

                                if (ctx.State) {
                                    ctx.RNG.Forward(ctx.State->TopologyCounter);
                                }

                                ui32 counterTopology = ctx.RNG.GetCounter();
                                auto initialGraph = generateTopology(ctx.RNG, n, mu, sigma);

                                for (ui64 j = 0; j < mcmcRepeats; ++ j) {
                                    TRelationGraph graph = initialGraph;

                                    if (ctx.State) {
                                        ctx.RNG.Forward(ctx.State->MCMCCounter);
                                    }

                                    ui32 counterMCMC = ctx.RNG.GetCounter();
                                    if (topologyName == "mcmc") {
                                        MCMCRandomize(ctx.RNG, graph);
                                    }

                                    for (ui64 k = 0; k < equiJoinKeysGenerationRepeats; ++ k) {
                                        if (ctx.State) {
                                            ctx.RNG.Forward(ctx.State->KeyCounter);
                                        }

                                        ui32 counterKeys = ctx.RNG.GetCounter();
                                        graph.SetupKeysPitmanYor(ctx.RNG, TPitmanYorConfig{.Alpha = alpha, .Theta = theta});

                                        std::string state = TBenchState(ctx.RNG.GetSeed(), counterTopology, counterMCMC, counterKeys).toHex();

                                        Cout << "\n\nReproduce: TOPOLOGY='";
                                        Cout << "type=" << topologyName << "; "
                                            << "N=" << n << "; "
                                            << "alpha=" << alpha << "; "
                                            << "theta=" << theta << "; "
                                            << "sigma=" << sigma << "; "
                                            << "mu=" << mu << "; "
                                             << "state=" << state << "'\n";

                                        try {
                                            auto result = BenchmarkShuffleEliminationOnTopology(config, ctx.Session, resultType, graph);
                                            if (!result) {
                                                goto stop;
                                            }

                                            AccumulateAllStats(aggregate, *result);
                                            std::string graphName = WriteGraph(ctx, idx, graph);

                                            std::stringstream params;
                                            params << idx ++ << "," << state << "," << graphName << "," << commonParams.str();
                                            WriteAllStats(ctx, "", header, params.str(), *result);
                                        } catch (std::exception &exc) {
                                            Cout << "Skipped run: " << exc.what() << "\n";
                                            continue;
                                        }

                                        if (ctx.State) {
                                            // We are running in reproducibility mode, stop immediately after case is reproduced
                                            return;
                                        }
                                    }
                                }
                            }

                            WriteAllStats(ctx, "aggregate-", headerAggregate, commonParams.str(), aggregate);
                        }
                    stop:;
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(Benchmark) {
        TArgs args{GetTestParam("TOPOLOGY")};
        if (!args.HasArg("N")) {
            // prevent this test from launching non-interactively
            return;
        }

        auto config = GetBenchmarkConfig(args);
        DumpBenchmarkConfig(Cout, config);

        TTestContext ctx = CreateTestContext(args, GetTestParam("SAVE_DIR"));

        RunBenches(ctx, config, args);
    }

    void CheckStats(TRunningStatistics& stats, ui32 n, double min, double max, double median, double mad, double q1, double q3, double iqr, double mean, double stdev) {
        const double TOLERANCE = 0.0001;

        UNIT_ASSERT_EQUAL(stats.GetN(), n);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMin(), min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMax(), max, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMedian(), median, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.CalculateMAD(), mad, TOLERANCE);

        auto statistics = stats.GetStatistics();
        UNIT_ASSERT_EQUAL(statistics.GetN(), n);
        UNIT_ASSERT_DOUBLES_EQUAL(statistics.GetMin(), min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(statistics.GetMax(), max, TOLERANCE);

        auto report = statistics.ComputeStatistics();
        UNIT_ASSERT_EQUAL(report.N, n);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Min, min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Max, max, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Median, median, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.MAD, mad, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Q1, q1, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Q3, q3, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.IQR, iqr, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Mean, mean, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Stdev, stdev, TOLERANCE);
    }

    Y_UNIT_TEST(TStatistics) {
        TRunningStatistics stats;

        stats.AddValue(1);
        stats.AddValue(1);
        CheckStats(stats, 2, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(1);
        CheckStats(stats, 3, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(1);
        CheckStats(stats, 4, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(3);
        CheckStats(stats, 5, 1, 3, 1, 0, 1, 1, 0, 1.4000, 0.8944);

        stats.AddValue(5);
        CheckStats(stats, 6, 1, 5, 1, 0, 1, 2.5000, 1.5000, 2, 1.6733);

        stats.AddValue(7);
        CheckStats(stats, 7, 1, 7, 1, 0, 1, 4, 3, 2.7143, 2.4300);

        stats.AddValue(7);
        CheckStats(stats, 8, 1, 7, 2, 1, 1, 5.5000, 4.5000, 3.2500, 2.7124);

        stats.AddValue(8);
        CheckStats(stats, 9, 1, 8, 3, 2, 1, 7, 6, 3.7778, 2.9907);

        stats.AddValue(100);
        CheckStats(stats, 10, 1, 100, 4, 3, 1, 7, 6, 13.4000, 30.5585);

        stats.AddValue(12);
        CheckStats(stats, 11, 1, 100, 5, 4, 1, 7.5000, 6.5000, 13.2727, 28.9934);

        stats.AddValue(15);
        CheckStats(stats, 12, 1, 100, 6, 5, 1, 9, 8, 13.4167, 27.6486);

        stats.AddValue(11);
        CheckStats(stats, 13, 1, 100, 7, 5, 1, 11, 10, 13.2308, 26.4800);

        stats.AddValue(18);
        CheckStats(stats, 14, 1, 100, 7, 5.5000, 1.5000, 11.7500, 10.2500, 13.5714, 25.4731);

        stats.AddValue(19);
        CheckStats(stats, 15, 1, 100, 7, 6, 2, 13.5000, 11.5000, 13.9333, 24.5865);

        stats.AddValue(14);
        CheckStats(stats, 16, 1, 100, 7.5000, 6.5000, 2.5000, 14.2500, 11.7500, 13.9375, 23.7528);

        stats.AddValue(21);
        CheckStats(stats, 17, 1, 100, 8, 7, 3, 15, 12, 14.3529, 23.0623);

        stats.AddValue(9);
        CheckStats(stats, 18, 1, 100, 8.5000, 6, 3.5000, 14.7500, 11.2500, 14.0556, 22.4092);

        stats.AddValue(25);
        CheckStats(stats, 19, 1, 100, 9, 6, 4, 16.5000, 12.5000, 14.6316, 21.9221);

        stats.AddValue(17);
        CheckStats(stats, 20, 1, 100, 10, 7, 4.5000, 17.2500, 12.7500, 14.7500, 21.3440);

        stats.AddValue(18);
        CheckStats(stats, 21, 1, 100, 11, 7, 5, 18, 13, 14.9048, 20.8156);

        stats.AddValue(10);
        CheckStats(stats, 22, 1, 100, 10.5000, 7, 5.5000, 17.7500, 12.2500, 14.6818, 20.3409);

        stats.AddValue(22);
        CheckStats(stats, 23, 1, 100, 11, 7, 6, 18, 12, 15, 19.9317);
    }

} // Y_UNIT_TEST_SUITE(KqpJoinTopology)

} // namespace NKikimr::NKqp
