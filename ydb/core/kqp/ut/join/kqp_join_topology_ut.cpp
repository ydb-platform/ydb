#include "kqp_join_topology_generator.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>
#include <util/generic/array_size.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_arg_parser.h>
#include <ydb/core/kqp/ut/common/kqp_tuple_parser.h>
#include <ydb/core/kqp/ut/common/kqp_aligner.h>
#include <ydb/core/kqp/ut/common/kqp_benches.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/join/kqp_join_hypergraph_generator.h>

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/yql/dq/opt/dq_opt_make_join_hypergraph.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based.h>

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

    std::string ConfigureQuery(const std::string& query, bool enableShuffleElimination = false, [[maybe_unused]]unsigned optLevel = 2) {
        std::string queryWithShuffleElimination = "PRAGMA ydb.OptShuffleElimination=\"";
        queryWithShuffleElimination += enableShuffleElimination ? "true" : "false";
        queryWithShuffleElimination += "\";\n";
        queryWithShuffleElimination += "PRAGMA ydb.CBOTimeout='" + std::to_string(UINT32_MAX) + "';\n";
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
        appConfig.MutableTableServiceConfig()->SetEnableOrderOptimizaionFSM(true);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(compilationTimeout.MilliSeconds());

        TKikimrSettings serverSettings(appConfig);
        serverSettings.SetWithSampleTables(false);
        serverSettings.SetKqpSettings(settings);

        return std::make_unique<TKikimrRunner>(serverSettings);
    }

    std::optional<std::map<std::string, TStatistics>>
    BenchmarkShuffleEliminationOnTopology(TBenchmarkConfig config, NYdb::NQuery::TSession session, std::string resultType, TRelationGraph graph) {
        Cout << "================================= GRAPH ==================================\n";
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
        OverrideWithArg<ui64>(prefix + ".MinRepeats", args, config.MinRepeats);
        OverrideWithArg<ui64>(prefix + ".MaxRepeats", args, config.MaxRepeats);
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

    struct TTestContext {
        std::unique_ptr<TKikimrRunner> Runner;
        NYdb::NQuery::TQueryClient QueryClient;
        NYdb::NQuery::TSession Session;

        TRNG RNG;

        std::string OutputDir;
        std::map<std::string, TUnbufferedFileOutput> Streams = {};
    };

    TTestContext CreateTestContext(TArgs args, std::string outputDir = "") {
        std::random_device randomDevice;
        TRNG rng(randomDevice() % UINT32_MAX);

        auto numTablesRanged = args.GetArg<ui64>("N");

        TSchema fullSchema = TSchema::MakeWithEnoughColumns(numTablesRanged.GetLast());
        TString stats = TSchemaStats::MakeRandom(rng, fullSchema, 7, 10).ToJSON();

        auto kikimr = GetCBOTestsYDB(stats, TDuration::Seconds(100000000));
        auto db = kikimr->GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        return {std::move(kikimr), std::move(db), std::move(session), std::move(rng), outputDir};
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

    using TTopologyFunction = TRelationGraph (*)(TRNG& rng, ui32 n, double mu, double sigma, TPitmanYorConfig config);
    template <auto TTrivialTopologyGenerator>
    TTopologyFunction GetTrivialTopology() {
        return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma, TPitmanYorConfig config) {
            return TTrivialTopologyGenerator(rng, n, config);
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
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma, TPitmanYorConfig config) {
                return GenerateRandomTree(rng, n, config);
            };
        }

        if (topologyName == "mcmc" || topologyName == "havel-hakimi") {
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma, TPitmanYorConfig config) {
                auto sampledDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                auto graphicDegrees = MakeGraphicConnected(sampledDegrees);
                auto initialGraph = ConstructGraphHavelHakimi(rng, graphicDegrees, config);

                return initialGraph;
            };
        }

        if (topologyName == "chung-lu") {
            return []([[maybe_unused]] TRNG& rng, ui32 n, [[maybe_unused]] double mu, [[maybe_unused]] double sigma, TPitmanYorConfig config) {
                auto initialDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                auto initialGraph = GenerateRandomChungLuGraph(rng, initialDegrees, config);

                return initialGraph;
            };
        }

        throw std::runtime_error("Unknown topology: '" + topologyName + "'");
    }

    void RunBenches(TTestContext& ctx, TBenchmarkConfig config, TArgs args) {
        std::string resultType = args.GetStringOrDefault("result", "SE");

        ui64 topologyGenerationRepeats = args.GetArgOrDefault<ui64>("gen-n", "1").GetValue();
        ui64 mcmcRepeats = args.GetArgOrDefault<ui64>("mcmc-n", "1").GetValue();
        ui64 equiJoinKeysGenerationRepeats = args.GetArgOrDefault<ui64>("keys-n", "1").GetValue();
        bool reorder = args.GetArgOrDefault<ui64>("reorder", "1").GetValue() != 0;

        std::string topologyName = args.GetStringOrDefault("type", "star");
        auto generateTopology = GetTopology(topologyName);

        std::string headerAggregate = "idx,N,alpha,theta,sigma,mu," + TComputedStatistics::GetCSVHeader();
        std::string header = "idx,state,graph_name,aggregate_" + headerAggregate;

        ui32 idx = 0;
        ui32 aggregateIdx = 0;

        auto configPitmanYor = GetPitmanYorConfig(args);

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

                                // TODO: only worked with previous RNG .Forward
                                // if (ctx.State) {
                                //     ctx.RNG.Forward(ctx.State->TopologyCounter);
                                // }

                                auto initialGraph = generateTopology(ctx.RNG, n, mu, sigma, configPitmanYor);

                                for (ui64 j = 0; j < mcmcRepeats; ++ j) {
                                    TRelationGraph graph = initialGraph;

                                    // TODO: only worked with previous RNG .Forward
                                    // if (ctx.State) {
                                    //     ctx.RNG.Forward(ctx.State->MCMCCounter);
                                    // }

                                    if (topologyName == "mcmc") {
                                        MCMCRandomizeDegreePreserving(ctx.RNG, graph, configPitmanYor);
                                    }

                                    for (ui64 k = 0; k < equiJoinKeysGenerationRepeats; ++ k) {
                                        // TODO: only worked with previous RNG .Forward
                                        // if (ctx.State) {
                                        //     ctx.RNG.Forward(ctx.State->KeyCounter);
                                        // }

                                        Cout << "\n\n";
                                        Cout << "Test #" << idx << "\n";
                                        Cout << "Reproduce: TOPOLOGY='";
                                        Cout << "type=" << topologyName << "; "
                                             << "N=" << n << "; "
                                             << "alpha=" << alpha << "; "
                                             << "theta=" << theta << "; ";

                                        if (topologyName == "mcmc" || topologyName == "chung-lu") {
                                            Cout << "sigma=" << sigma << "; "
                                                 << "mu=" << mu << "; ";
                                        }

                                        if (args.HasArg("reorder")) {
                                            Cout << "reorder=" << reorder << "; ";
                                        }

                                        // Cout << "state=" << state << "'\n";

                                        try {
                                            if (reorder) {
                                                Cout << "================================= GRAPH BEFORE REODERING =================\n";
                                                graph.DumpGraph(Cout);
                                                graph.ReorderDFS();
                                            }

                                            auto result = BenchmarkShuffleEliminationOnTopology(config, ctx.Session, resultType, graph);
                                            if (!result) {
                                                goto stop;
                                            }

                                            AccumulateAllStats(aggregate, *result);
                                            std::string graphName = WriteGraph(ctx, idx, graph);

                                            std::stringstream params;
                                            params << idx ++ << "," << graphName << "," << commonParams.str();
                                            WriteAllStats(ctx, "", header, params.str(), *result);
                                        } catch (std::exception &exc) {
                                            Cout << "Skipped run: " << exc.what() << "\n";
                                            continue;
                                        }

                                        // if (ctx.State) {
                                        //     // We are running in reproducibility mode, stop immediately after case is reproduced
                                        //     return;
                                        // }
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


    class BenchRunner {
    public:
        using THypergraphNodes = std::bitset<64>;
        using TJoinHypergraph = NYql::NDq::TJoinHypergraph<THypergraphNodes>;
        using TJoinTree = std::shared_ptr<NYql::IBaseOptimizerNode>;
        using TOrderingsPtr = TSimpleSharedPtr<NYql::NDq::TOrderingsStateMachine>;

        BenchRunner(const TBenchmarkConfig& config, TUnbufferedFileOutput& output, const TTupleParser::TTable& args)
            : Config_(config)
            , Output_(output)
            , Args_(args)
            , TotalTasks_(args.size())
            , Aligner_(1, "&") // Initialize aligner with & delimiter
        {
        }

        void Launch() {
            int maxCores = std::thread::hardware_concurrency();
            if (maxCores == 0) maxCores = 4;

            Cout << "Starting Benchmark with " << maxCores << " threads..." << Endl;

            std::atomic<bool> stopMonitor{false};
            std::thread monitor(&BenchRunner::MonitorDashboard, this, maxCores, std::ref(stopMonitor));

            std::vector<std::thread> workers;
            workers.reserve(maxCores);
            for (int i = 0; i < maxCores; ++i) {
                workers.emplace_back(&BenchRunner::WorkerRoutine, this);
            }

            for (auto& t : workers) {
                if (t.joinable()) t.join();
            }

            stopMonitor = true;
            if (monitor.joinable()) monitor.join();
        }

    private:
        void WorkerRoutine() {
            while (true) {
                ui64 idx = NextTaskIdx_.fetch_add(1);
                if (idx >= TotalTasks_) return;

                ActiveThreads_.fetch_add(1);
                ProcessRow(idx);
                ActiveThreads_.fetch_sub(1);

                CompletedTasks_.fetch_add(1);
            }
        }

        void ProcessRow(ui64 idx) {
            TParamsMap params = Args_[idx];

            std::string label = params.GetValue("label");
            ui64 n = params.GetValue<ui64>("N");

            if (IsKnownTimeout(label, n)) {
                return;
            }

            std::string savedSeed = params.GetValue<std::string>("seed", "");

            TRNG rng(0);
            if (!savedSeed.empty()) {
                rng.RestoreFromHex(savedSeed);
            } else {
                // If no seed is provided, just create a random one
                std::random_device randomDevice;
                rng = TRNG(randomDevice() % UINT32_MAX);
            }

            try {
                auto pyConfig = GetPitmanYor(params);
                auto baseGraph = GenTopo(rng, params);

                ui32 degreePreservingVariants = params.GetValue<ui32>("mcmc-degree", 0);
                ui32 edgePreservingVariants = params.GetValue<ui32>("mcmc-edge", 0);

                rng.Mark(0);

                ProcessSingleGraph(rng, idx, params, baseGraph, 0);

                TRelationGraph graphDP = baseGraph;
                for (ui32 i = 1; i <= degreePreservingVariants; ++i) {
                    if (IsKnownTimeout(label, n)) break;

                    rng.Rewind(0);
                    MCMCRandomizeDegreePreserving(rng, graphDP, pyConfig, TMCMCConfig::Perturbation());
                    ProcessSingleGraph(rng, idx, params, graphDP, i);
                }

                TRelationGraph graphEP = baseGraph;
                for (ui32 i = 1; i <= edgePreservingVariants; ++i) {
                    if (IsKnownTimeout(label, n)) break;

                    rng.Rewind(0);
                    MCMCRandomizeEdgePreserving(rng, graphEP, pyConfig, TMCMCConfig::Perturbation());
                    ProcessSingleGraph(rng, idx, params, graphEP, degreePreservingVariants + i);
                }

            } catch (const std::exception& e) {
                LogMessage(idx, n, label, "[ERROR] " + std::string(e.what()));
            }
        }

        void LogMessage(ui64 taskIdx, ui64 n, const std::string& label, const std::string& msg) {
            std::stringstream ss;
            ss << "(" << (taskIdx + 1) << "/" << TotalTasks_ << ")"
               << "&N = " << n
               << "&Label = " << label << " &"
               << msg;

            EnqueueLog(Aligner_.Align(ss.str()));
        }

        void ProcessSingleGraph(
            TRNG& rng,
            ui64 taskIdx,
            const TParamsMap& params,
            const TRelationGraph& graph,
            ui32 variantIdx)
        {
            std::string label = params.GetValue("label");
            ui64 n = params.GetValue<ui64>("N");

            rng.Mark(1);
            std::string seed = rng.SerializeToHex();

            std::stringstream ss;
            ss << "(" << (taskIdx + 1) << "/" << TotalTasks_ << ")";
            if (variantIdx > 0) {
                ss << "&[v" << variantIdx << "]";
            } else {
                ss << "&";
            }
            ss << "&N = " << n
            << "&Label = " << label << " &";

            auto tree = GenerateJoinTree(rng, graph, params);
            RandomizeJoinTypes(rng, tree, params);

            auto hypergraph = BuildHypergraph(tree);
            auto orderings = BuildOrderingsFSM(hypergraph);

            auto stats = BenchOptimizer(tree, orderings);
            if (stats) {
                auto computed = stats->ComputeStatistics();

                WriteJsonResult(seed, params, computed, graph, tree, hypergraph, variantIdx);
                ss << "Time = " << TimeFormatter::Format(computed.Median, computed.MAD, "&±");

                if (computed.Median > Config_.SingleRunTimeout) {
                    RegisterTimeout(label, n);
                }
            } else {
                ss << "[TIMEOUT]";
                RegisterTimeout(label, n);
            }

            EnqueueLog(Aligner_.Align(ss.str()));
        }

        TPitmanYorConfig GetPitmanYor(const TParamsMap& params) {
            double alpha = params.GetValue<double>("alpha");
            double theta = params.GetValue<double>("theta");
            double assortativity = params.GetValue<double>("assort");
            return TPitmanYorConfig{.Alpha = alpha, .Theta = theta, .Assortativity = assortativity};
        }

        TRelationGraph GenTopo(TRNG &rng, const TParamsMap& params) {
            ui64 n = params.GetValue<ui64>("N");
            std::string topologyName = params.GetValue("type");
            auto config = GetPitmanYor(params);

            if (topologyName == "star") {
                return GenerateStar(rng, n, config);
            }

            if (topologyName == "path") {
                return GeneratePath(rng, n, config);
            }

            if (topologyName == "clique") {
                return GenerateClique(rng, n, config);
            }

            if (topologyName == "random-tree") {
                return GenerateRandomTree(rng, n, config);
            }

            if (topologyName == "mcmc" || topologyName == "havel-hakimi") {
                double mu = params.GetValue<double>("mu");
                double sigma = params.GetValue<double>("sigma");
                auto sampledDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                auto graphicDegrees = MakeGraphicConnected(sampledDegrees);
                auto initialGraph = ConstructGraphHavelHakimi(rng, graphicDegrees, config);
                auto graph = initialGraph;
                if (topologyName == "mcmc") {
                    MCMCRandomizeDegreePreserving(rng, graph, config);
                }
                ForceReconnection(rng, graph, config);
                return graph;
            }

            if (topologyName == "chung-lu") {
                double mu = params.GetValue<double>("mu");
                double sigma = params.GetValue<double>("sigma");
                auto sampledDegrees = GenerateLogNormalDegrees(rng, n, mu, sigma);
                auto initialGraph = GenerateRandomChungLuGraph(rng, sampledDegrees, config);
                auto graph = initialGraph;
                ForceReconnection(rng, graph, config);
                return graph;
            }

            throw std::runtime_error("Unknown topology: '" + topologyName + "'");
        }

        void RandomizeJoinTypes(TRNG &rng, TJoinTree& tree, const TParamsMap& params) {
            static const std::vector<std::pair<std::string, NYql::EJoinKind>> joins = {
                { "inner-join",      NYql::EJoinKind::InnerJoin },
                { "left-join",       NYql::EJoinKind::LeftJoin  },
                { "right-join",      NYql::EJoinKind::RightJoin },
                { "outer-join",      NYql::EJoinKind::OuterJoin },
                { "cross-join",      NYql::EJoinKind::Cross     },
                { "left-only-join",  NYql::EJoinKind::LeftOnly  },
                { "right-only-join", NYql::EJoinKind::RightOnly },
                { "left-semi-join",  NYql::EJoinKind::LeftSemi  },
                { "right-semi-join", NYql::EJoinKind::RightSemi },
                { "exclusion-join",  NYql::EJoinKind::Exclusion }
            };

            std::map<NYql::EJoinKind, double> probabilities;
            for(const auto& [name, kind] : joins) {
                probabilities[kind] = params.GetValue<double>(name);
            }

            NKikimr::NKqp::RandomizeJoinTypes(rng, tree, probabilities);
        }

        TJoinTree GenerateJoinTree(TRNG& rng, const TRelationGraph& graph, const TParamsMap& params) {
            double bushiness = params.GetValue<double>("bushiness");
            return ToJoinTree(rng, graph, bushiness);
        }

        TJoinHypergraph BuildHypergraph(TJoinTree tree) {
            return NYql::NDq::MakeJoinHypergraph<THypergraphNodes>(tree);
        }

        TOrderingsPtr BuildOrderingsFSM(TJoinHypergraph hypergraph) {
            NYql::NDq::TOrderingsStateMachineConstructor<THypergraphNodes> orderings(hypergraph);
            auto stateMachine = orderings.Construct();
            return new NYql::NDq::TOrderingsStateMachine(stateMachine);
        }

        std::optional<TStatistics> BenchOptimizer(TJoinTree tree, TOrderingsPtr orderingsFSM) {
            NYql::TCBOSettings settings{ .MaxDPhypDPTableSize = UINT32_MAX };
            NYql::TBaseProviderContext ctx;
            NYql::TExprContext ectx;

            auto optimizer = std::unique_ptr<NYql::IOptimizerNew>(
                NYql::NDq::MakeNativeOptimizerNew(ctx, settings, ectx, /*enableShuffleElimination=*/true, orderingsFSM)
            );

            return Benchmark(Config_, [&]() -> bool {
                try {
                    if (tree->Kind == NYql::EOptimizerNodeKind::JoinNodeType) {
                        auto join = std::static_pointer_cast<NYql::TJoinOptimizerNode>(tree);
                        auto plan = optimizer->JoinSearch(join);
                        return !!plan;
                    }
                } catch (...) {
                    return false;
                }
                return true;
            });
        }

        // Templated stats to accept whatever ComputeStatistics returns (TStatistics or TRes)
        template <typename TStats>
        void WriteJsonResult(const std::string& seed, const TParamsMap& params, 
                             const TStats& timeStats, const TRelationGraph& graph,
                             const TJoinTree& query, TJoinHypergraph& hyper, ui32 variantIdx) {

            ui64 tableIdx = params.GetValue<ui64>("idx");

            ui64 n = params.GetValue<ui64>("N");
            std::string label = params.GetValue("label");

            NJson::TJsonValue entry(NJson::JSON_MAP);
            entry.InsertValue("idx", NJson::TJsonValue(EntryIdx_++));
            entry.InsertValue("table-id", NJson::TJsonValue(tableIdx));
            entry.InsertValue("N", NJson::TJsonValue(n));
            entry.InsertValue("label", NJson::TJsonValue(label));
            entry.InsertValue("variant", NJson::TJsonValue(variantIdx));
            entry.InsertValue("time", timeStats.ToJson());
            entry.InsertValue("seed", NJson::TJsonValue(seed));

            entry.InsertValue("query", TOptimizerNodeSerializer::Serialize(query));
            entry.InsertValue("graph", TRelationGraphSerializer::Serialize(graph));
            entry.InsertValue("hypergraph", TJoinHypergraphSerializer<THypergraphNodes>::Serialize(hyper));

            std::lock_guard<std::mutex> lock(MtxOutput_);
            NJsonWriter::TBuf writer(NJsonWriter::HEM_ESCAPE_HTML, &Output_);
            writer.SetIndentSpaces(0);
            writer.WriteJsonValue(&entry, false);
            Output_ << "\n"; // Implicit flush
        }

        bool IsKnownTimeout(std::string label, ui64 n) {
            std::lock_guard<std::mutex> lock(MtxTimeouts_);
            if (TimeoutedNs_.count(label)) {
                if (n > TimeoutedNs_[label]) {
                    return true;
                }
            }
            return false;
        }

        void RegisterTimeout(std::string label, ui64 n) {
            std::lock_guard<std::mutex> lock(MtxTimeouts_);
            if (TimeoutedNs_.count(label) == 0 || n < TimeoutedNs_[label]) {
                TimeoutedNs_[label] = n;
            }
        }

        void EnqueueLog(const std::string& msg) {
            if (!msg.empty()) {
                std::lock_guard<std::mutex> lock(MtxConsole_);
                MessageQueue_.push(msg);
            }
        }

        void MonitorDashboard(int maxCores, std::atomic<bool>& stopSignal) {
            while (!stopSignal || CompletedTasks_ < TotalTasks_) {
                {
                    std::lock_guard<std::mutex> lock(MtxConsole_);
                    if (!MessageQueue_.empty()) {
                        std::cout << "\33[2K\r";
                        while (!MessageQueue_.empty()) {
                            std::cout << MessageQueue_.front() << "\n";
                            MessageQueue_.pop();
                        }
                    }
                }

                ui64 done = CompletedTasks_.load();
                int active = ActiveThreads_.load(); // Read Active Count

                float ratio = (TotalTasks_ > 0) ? (float)done / TotalTasks_ : 0.0f;
                int width = 60; // Slightly smaller bar to fit Active info

                std::cout << "\33[2K\r[";
                int pos = width * ratio;
                for (int i = 0; i < width; ++i) {
                    if (i < pos) std::cout << "=";
                    else if (i == pos) std::cout << ">";
                    else std::cout << " ";
                }
                std::cout << "] " << int(ratio * 100.0) << "% "
                        << "| Active: " << active << "/" << maxCores << " " // Show Active Jobs
                        << "| Done: " << done << "/" << TotalTasks_
                        << std::flush;

                if (done == TotalTasks_) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            std::cout << "\n";
        }


        const TBenchmarkConfig& Config_;
        TUnbufferedFileOutput& Output_; // Reference is crucial
        const TTupleParser::TTable& Args_;
        size_t TotalTasks_;

        std::atomic<ui64> NextTaskIdx_{0};
        std::atomic<ui64> CompletedTasks_{0};
        std::atomic<ui64> EntryIdx_{0};

        std::atomic<int> ActiveThreads_{0};
        StreamAligner Aligner_;

        std::mutex MtxOutput_;
        std::mutex MtxConsole_;
        std::mutex MtxTimeouts_;

        std::queue<std::string> MessageQueue_;
        std::map<std::string, ui64> TimeoutedNs_;
    };


    Y_UNIT_TEST(Dataset) {
        std::string benchArgs = GetTestParam("BENCHMARK");
        auto config = GetBenchmarkConfig(TArgs{benchArgs});
        DumpBenchmarkConfig(Cout, config);

        std::string args = GetTestParam("DATASET");
        if (args.empty()) {
            std::string datasetFile = GetTestParam("DATASET_FILE");
            if (datasetFile.empty()) {
                Cerr << "Filename required: --test-param DATASET_FILE='<filename>'" << Endl;
                return;
            }
            args = TFileInput(datasetFile).ReadAll();
        }

        auto parameters = TTupleParser{args}.Parse();

        // Fix: Use GetValue instead of manual vector iteration/indexing
        // NKikimr::NKqp::TParamsMap usually supports GetValue<T>(name)
        auto findN = [](const auto& row) -> ui64 {
            try {
                return row.template GetValue<ui64>("N");
            } catch (...) {
                // Fallback if the map stores everything as strings
                return std::stoull(row.GetValue("N"));
            }
        };

        std::stable_sort(parameters.begin(), parameters.end(), [&](const auto& lhs, const auto& rhs) {
            return findN(lhs) < findN(rhs);
        });

        Cout << "\n";
        // PrintTable(parameters); // Assuming this function exists in env

        std::string outputFile = GetTestParam("OUTPUT");
        if (outputFile.empty()) {
            ythrow yexception() << "Output required: --test-param OUTPUT='<filename>'";
        }

        // Initialize file stream here so it persists for the lifetime of runner
        TUnbufferedFileOutput outFileStream(outputFile.c_str());

        BenchRunner runner(config, outFileStream, parameters);
        runner.Launch();
    }

    struct CustomQuery {
        std::string Query;
        std::string CreateSchema;
        std::string DropSchema;
        std::string Stats;
    };

    CustomQuery CreateCustomQuery(TRNG& rng, const std::string& query) {
        std::uniform_int_distribution distribution(7, 10);

        std::regex tableAttributeRegex("([A-Z]+[A-Z0-9_]*)\\.([a-z]+[a-z0-9]*)");
        auto it = std::sregex_iterator(query.begin(), query.end(), tableAttributeRegex);

        std::string updatedQuery;
        std::map<std::string, std::set<std::string>> tables;

        auto getAttributeName = [](std::string relationName, std::string name) {
            std::string lowerName = relationName;
            std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(),
                           [](unsigned char symbol){ return std::tolower(symbol); });

            return lowerName + "_" + name;
        };

        size_t lastPosition = 0;
        for (auto end = std::sregex_iterator(); it != end; ++ it) {
            std::string relationName = (*it)[1];
            std::string rowName = getAttributeName(relationName, (*it)[2].str());
            tables[relationName].insert(rowName);

            updatedQuery += it->prefix().str() + relationName + "." + rowName;
            lastPosition = it->position() + it->length();
        }

        updatedQuery += query.substr(lastPosition);

        std::regex relationRegex("[A-Z]+[A-Z0-9_]*");
        for (auto end = std::sregex_iterator(),
                  it = std::sregex_iterator(query.begin(), query.end(), relationRegex);
             it != end; ++ it) {

            std::string relationName = (*it)[0].str();
            if (tables[relationName].empty()) {
                tables[relationName].insert(getAttributeName(relationName, "a"));
            }
        }

        std::regex joinClauseRegex(
            "(from|(((left|right|full)\\s+)?((outer|inner)\\s+)?((semi|anti)\\s+)?|cross\\s+)?join)");
        updatedQuery = std::regex_replace(updatedQuery, joinClauseRegex, "\n$0");

        std::string createSchema;
        std::string dropSchema;
        std::stringstream stats;

        bool isFirst = true;

        stats << "{\n";
        for (auto& [name, rows] : tables) {
            std::string tablePath = "/Root/" + name;

            dropSchema += "drop table `" + tablePath + "`;\n";
            createSchema += "create table `" + tablePath + "` (\n";

            if (!isFirst) {
                stats << ",\n";
            }

            isFirst = false;

            stats << "    \"" << tablePath << "\": {\n";
            stats << "        \"" << "n_rows" << "\": " << std::pow(10, distribution(rng)) << ",\n";
            stats << "        \"" << "byte_size" << "\": " << 64 * std::pow(10, distribution(rng)) << "\n";
            stats << "    }";

            std::string pk;
            for (const std::string& row : rows) {
                std::string attribute = row;
                if (pk.empty()) {
                    pk = attribute;
                }

                createSchema += "    " + attribute + " int32 not null,\n";
            }

            createSchema += "    primary key (" + pk + ")\n";
            createSchema += ") with (store = column);\n";
        }
        stats << "\n}\n";

        return {std::move(updatedQuery), std::move(createSchema), std::move(dropSchema), stats.str()};
    }


    Y_UNIT_TEST(CustomBenchmark) {
        std::string query = GetTestParam("QUERY");
        if (query.empty()) {
            return;
        }

        std::random_device randomDevice;
        TRNG rng(randomDevice() % UINT32_MAX);
        auto customQuery = CreateCustomQuery(rng, query);

        Cout <<   "--------------------------- (1) Stats:\n" << customQuery.Stats << "\n";

        auto kikimr = GetCBOTestsYDB(TString(customQuery.Stats), TDuration::Seconds(100000));
        auto db = kikimr->GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        std::string benchArgs = GetTestParam("BENCHMARK");
        auto config = GetBenchmarkConfig(TArgs{benchArgs});
        DumpBenchmarkConfig(Cout, config);

        Cout << "\n--------------------------- (2) Create schema:\n" << customQuery.CreateSchema;
        ExecuteQuery(session, customQuery.CreateSchema);

        Cout << "\n--------------------------- (3) Passed query:\n" << query << "\n";
        Cout << "\n--------------------------- (4) Substituted query:\n" << customQuery.Query << "\n";
        std::string configuredQuery = ConfigureQuery(customQuery.Query, /*enableShuffleElimination=*/true, /*optLevel=*/2);
        Cout << "\n--------------------------- (5) Processed query:\n" << configuredQuery << "\n";

        Cout << "\n";
        BenchmarkExplain(config, session, TString(configuredQuery));

        Cout << "\n--------------------------- (6) Drop schema:\n" << customQuery.DropSchema;
        ExecuteQuery(session, customQuery.DropSchema);
    }

} // Y_UNIT_TEST_SUITE(KqpJoinTopology)

} // namespace NKikimr::NKqp
