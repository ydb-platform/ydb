#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_benches.h>
#include <ydb/core/kqp/ut/common/kqp_arg_parser.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/testing/common/env.h>

#include "kqp_join_topology_generator.h"


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpJoinTopology) {

    std::optional<TString> ExplainQuery(NYdb::NQuery::TSession session, const std::string &query) {
        auto explainRes = session.ExecuteQuery(query,
          NYdb::NQuery::TTxControl::NoTx(),
          NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        if (explainRes.GetStatus() == EStatus::TIMEOUT) {
            return std::nullopt;
        }

        explainRes.GetIssues().PrintTo(Cout);
        UNIT_ASSERT_VALUES_EQUAL(explainRes.GetStatus(), EStatus::SUCCESS);

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

    void JustPrintPlan(const TString &plan) {
      NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
          NYdb::NConsoleClient::EDataFormat::PrettyTable,
          /*analyzeMode=*/true, Cout, /*maxWidth=*/0
      );

      queryPlanPrinter.Print(plan);
    }

    std::string ConfigureQuery(const std::string &query, bool enableShuffleElimination = false, unsigned optLevel = 2) {
        std::string queryWithShuffleElimination = "PRAGMA ydb.OptShuffleElimination=\"";
        queryWithShuffleElimination += enableShuffleElimination ? "true" : "false";
        queryWithShuffleElimination += "\";\n";
        queryWithShuffleElimination += "PRAGMA ydb.MaxDPHypDPTableSize='4294967295';\n";
        queryWithShuffleElimination += "PRAGMA ydb.ShuffleEliminationJoinNumCutoff='" + std::to_string(UINT32_MAX) + "';\n";
        queryWithShuffleElimination += "PRAGMA ydb.CostBasedOptimizationLevel=\"" + std::to_string(optLevel) + "\";\n";
        queryWithShuffleElimination += query;

        return queryWithShuffleElimination;
    }

    std::optional<TRunningStatistics<ui64>> BenchmarkExplain(TBenchmarkConfig config, NYdb::NQuery::TSession session, const TString& query) {
        std::optional<TString> savedPlan = std::nullopt;
        auto stats = Benchmark(config, [&]() -> bool {
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

        assert(savedPlan);
        JustPrintPlan(*savedPlan);
        Cout << "--------------------------------------------------------------------------\n";

        DumpTimeStatistics(stats->GetStatistics(), Cout);

        return stats;
    }


    struct ShuffleEliminationBenchmarkResult {
        TRunningStatistics<ui64> WithoutCBO;
        TRunningStatistics<i64> WithShuffleElimination;
        TRunningStatistics<i64> WithoutShuffleElimination;
    };

    std::optional<TRunningStatistics<double>> BenchmarkShuffleElimination(TBenchmarkConfig config, NYdb::NQuery::TSession session, const TString& query) {
        Cout << "--------------------------------- W/O CBO --------------------------------\n";
        auto withoutCBO = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/0));
        if (!withoutCBO) {
            return std::nullopt;
        }

        Cout << "--------------------------------- CBO-SE ---------------------------------\n";
        auto withoutShuffleElimination = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/2));
        if (!withoutShuffleElimination) {
            return std::nullopt;
        }

        Cout << "--------------------------------- CBO+SE ---------------------------------\n";
        auto withShuffleElimination = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/true,  /*optLevel=*/2));
        if (!withShuffleElimination) {
            return std::nullopt;
        }

        Cout << "--------------------------------------------------------------------------\n";


        ShuffleEliminationBenchmarkResult result {
            .WithoutCBO = *withoutCBO,
            .WithShuffleElimination = *withShuffleElimination - *withoutCBO,
            .WithoutShuffleElimination = *withoutShuffleElimination - *withoutCBO
        };

        return result.WithShuffleElimination / result.WithoutShuffleElimination;
    }

    std::unique_ptr<TKikimrRunner> GetCBOTestsYDB(TString stats, TDuration compilationTimeout) {
        TVector<NKikimrKqp::TKqpSetting> settings;

        assert(!stats.empty());

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

    std::optional<TRunningStatistics<double>> BenchmarkShuffleEliminationOnTopology(TBenchmarkConfig config, NYdb::NQuery::TSession session, TRelationGraph graph) {
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

        Cout << "================================= BENCHMARK ==============================\n";
        TString query = graph.MakeQuery();
        Cout << query;
        auto resultTime = BenchmarkShuffleElimination(config, session, query);

        Cout << "================================= FINALIZE ===============================\n";
        auto deletionQuery = graph.GetSchema().MakeDropQuery();
        Cout << deletionQuery;
        if (!ExecuteQuery(session, deletionQuery)) { // TODO: this is really bad probably?
            return std::nullopt;
        }
        Cout << "==========================================================================\n";

        return resultTime;
    }


    template <typename TValue>
    void OverrideWithArg(std::string key, TArgs args, auto& value) {
        if (args.HasArg(key)) {
            value = args.GetArg<TValue>(key).GetValue();
        }
    }

    void OverrideRepeatedTestConfig(std::string prefix, TArgs args, TRepeatedTestConfig &config) {
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

    void DumpBenchmarkConfig(IOutputStream &OS, TBenchmarkConfig config) {
        OS << "config = {\n";
        OS << "    .Warmup = {\n";
        OS << "         .MinRepeats = " << config.Warmup.MinRepeats << ",\n";
        OS << "         .MaxRepeats = " << config.Warmup.MaxRepeats << ",\n";
        OS << "         .Timeout = " << TimeFormatter::Format(config.Warmup.Timeout) << "\n";
        OS << "    },\n\n";
        OS << "    .Bench = {\n";
        OS << "        .MinRepeats = " << config.Bench.MinRepeats << ",\n";
        OS << "        .MaxRepeats = " << config.Bench.MaxRepeats << ",\n";
        OS << "        .Timeout = " << TimeFormatter::Format(config.Bench.Timeout) << "\n";
        OS << "    },\n\n";
        OS << "    .SingleRunTimeout = " << TimeFormatter::Format(config.SingleRunTimeout) << ",\n";
        OS << "    .MADThreshold = " << config.MADThreshold << "\n";
        OS << "}\n";
    }


    class TDegreeDistributionGenerator {
    public:
        virtual std::vector<int> Initialize(TArgs args) = 0;
        virtual std::vector<int> GenerateDegreeSequence(TRNG &rng) = 0;

        virtual ~TDegreeDistributionGenerator() = default;
    };

    class TTopologyTester {
    public:
        virtual void Initialize(TArgs args) = 0;
        virtual TRelationGraph ProduceGraph(TRNG &rng) = 0;
        virtual void DumpParamsHeader(IOutputStream &OS) = 0;
        virtual void DumpParams(IOutputStream &OS) = 0;
        virtual void Loop(std::function<void()>) {}

        virtual ~TTopologyTester() = default;
    };


    TPitmanYorConfig GetPitmanYorConfig(TArgs args) {
        return TPitmanYorConfig{
            .Alpha = args.GetArgOrDefault<double>("alpha", "0.5").GetValue(),
            .Theta = args.GetArgOrDefault<double>("theta", "1.0").GetValue()
        };
    }

    class TMCMCTester : public TTopologyTester {
    public:
        void Initialize(TArgs args) override {
            N_ = args.GetArg<ui64>("N").GetValue();

            auto initialDegrees = GenerateLogNormalDegrees(N_);
            auto fixedDegrees = MakeGraphicConnected(initialDegrees);
            InitialGraph_ = ConstructGraphHavelHakimi(fixedDegrees);
        }

        TRelationGraph ProduceGraph(TRNG &rng) override {
            TRelationGraph graph = InitialGraph_;
            MCMCRandomize(rng, graph, /*MCMCSteps*/100);
            return graph;
        }

        void DumpParamsHeader(IOutputStream &OS) override {
            OS << "N,";
        }

        void DumpParams(IOutputStream &OS) override {
            OS << N_ << ",";
        }

    private:
        TRelationGraph InitialGraph_;
        ui32 N_;
    };

    struct TTestContext {
        std::unique_ptr<TKikimrRunner> Runner;
        NYdb::NQuery::TQueryClient QueryClient;
        NYdb::NQuery::TSession Session;

        TRNG RNG;
        std::optional<TUnbufferedFileOutput> OS;
    };

    TTestContext CreateTestContext(TArgs args, uint64_t state = 0, std::string outputFile = "") {
        TRNG rng = TRNG::Deserialize(state);

        auto numTablesRanged = args.GetArg<uint64_t>("N");

        rng.reset(); // ensure this setup is always the same
        TSchema fullSchema = TSchema::MakeWithEnoughColumns(numTablesRanged.GetLast());
        TString stats = TSchemaStats::MakeRandom(rng, fullSchema, 7, 10).ToJSON();

        rng.Restore(state);

        auto kikimr = GetCBOTestsYDB(stats, TDuration::Seconds(10));
        auto db = kikimr->GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        std::optional<TUnbufferedFileOutput> os = std::nullopt;
        if (!outputFile.empty()) {
            os = TUnbufferedFileOutput(outputFile);
        }

        return { std::move(kikimr), std::move(db), std::move(session), std::move(rng), std::move(os) };
    }

    void RunMCMC(TTestContext &ctx, TBenchmarkConfig config, TArgs args) {
        if (ctx.OS) {
            (*ctx.OS) << "N,alpha,theta,logStdDev,logMean,repeats,seed,";
            DumpBoxPlotCSVHeader(*ctx.OS);
            (*ctx.OS) << "\n";
        }

        ui64 mcmcSteps = args.GetArgOrDefault<uint64_t>("mcmcSteps", "100").GetValue();
        for (ui64 n : args.GetArg<uint64_t>("N")) {
            for (double alpha : args.GetArgOrDefault<double>("alpha", "0.5")) {
                for (double theta : args.GetArgOrDefault<double>("theta", "1.0")) {
                    for (double logStdDev : args.GetArgOrDefault<double>("logStdDev", "0.5")) {
                        for (double logMean : args.GetArgOrDefault<double>("logMean", "1.0")) {
                            auto initialDegrees = GenerateLogNormalDegrees(n, logMean, logStdDev);
                            auto fixedDegrees = MakeGraphicConnected(initialDegrees);
                            auto initialGraph = ConstructGraphHavelHakimi(fixedDegrees);

                            ui64 repeats = args.GetArgOrDefault<uint64_t>("repeats", "1").GetValue();
                            for (ui64 i = 0; i < repeats; ++ i) {
                                Cout << "Reproduce: 'N=" << n << "; alpha=" << alpha << "; theta="
                                     << theta << "; logStdDev=" << logStdDev << "; logMean=" << logMean
                                     << "; seed=" << ctx.RNG.Serialize() << "'\n";

                                ui64 seed = ctx.RNG.Serialize();

                                TRelationGraph graph = initialGraph;
                                MCMCRandomize(ctx.RNG, graph, mcmcSteps);
                                graph.SetupKeysPitmanYor(ctx.RNG, TPitmanYorConfig{.Alpha = alpha, .Theta = theta});

                                auto result = BenchmarkShuffleEliminationOnTopology(config, ctx.Session, graph);
                                if (!result) {
                                    goto stop;
                                }


                                if (ctx.OS) {
                                    (*ctx.OS) << n << "," << alpha << "," << theta << "," << logStdDev
                                              << "," << logMean << "," << repeats << "," << seed << ",";
                                    DumpBoxPlotToCSV(*ctx.OS, result->GetStatistics());
                                    (*ctx.OS) << "\n";
                                }
                            }
                        }
                    }
                }
            }
        }
    stop:;
    }

    template <auto TrivialTopology>
    void RunTrivialTopology(TTestContext &ctx, TBenchmarkConfig config, TArgs args) {
        if (ctx.OS) {
            (*ctx.OS) << "N,alpha,theta,seed,";
            DumpBoxPlotCSVHeader(*ctx.OS);
            (*ctx.OS) << "\n";
        }

        for (ui64 n : args.GetArg<uint64_t>("N")) {
            for (double alpha : args.GetArgOrDefault<double>("alpha", "0.5")) {
                for (double theta : args.GetArgOrDefault<double>("theta", "1.0")) {
                    ui64 seed = ctx.RNG.Serialize();

                    Cout << "Reproduce: 'N=" << n << "; alpha=" << alpha << "; theta="
                         << theta << "; seed=" << seed << "'\n";

                    TRelationGraph graph = TrivialTopology(ctx.RNG, n);
                    graph.SetupKeysPitmanYor(ctx.RNG, TPitmanYorConfig{.Alpha = alpha, .Theta = theta});

                    auto result = BenchmarkShuffleEliminationOnTopology(config, ctx.Session, graph);
                    if (!result) {
                        goto stop;
                    }

                    if (ctx.OS) {
                        (*ctx.OS) << n << "," << alpha << "," << theta << "," << seed << ",";
                        DumpBoxPlotToCSV(*ctx.OS, result->GetStatistics());
                        (*ctx.OS) << "\n";
                    }
                }
            }
        }
    stop:;
    }

    Y_UNIT_TEST(Benchmark) {
        TArgs args{GetTestParam("TOPOLOGY")};
        if (!args.HasArg("N")) {
            return;
        }

        std::string topology = "star";
        if (args.HasArg("type")) {
            topology = args.GetString("type");
        }

        auto config = GetBenchmarkConfig(args);
        DumpBenchmarkConfig(Cout, config);

        uint64_t state = 0;
        if (args.HasArg("seed")) {
            state = args.GetArg<uint64_t>("seed").GetValue();
        }

        TTestContext ctx = CreateTestContext(args, state, GetTestParam("SAVE_FILE"));

        if (topology == "mcmc") {
            RunMCMC(ctx, config, args);
        } else if (topology == "star") {
            RunTrivialTopology<GenerateStar>(ctx, config, args);
        } else if (topology == "path") {
            RunTrivialTopology<GenerateLine>(ctx, config, args);
        } else if (topology == "clique") {
            RunTrivialTopology<GenerateFullyConnected>(ctx, config, args);
        }
    }


    Y_UNIT_TEST(TRunningStatistics) {
        TRunningStatistics<ui64> stats;

        stats.AddValue(1);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), 1);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 1 / 1.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 1);

        stats.AddValue(1);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), (1 + 1) / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 2 / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 2);

        stats.AddValue(1);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), 1);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 3 / 3.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 3);

        stats.AddValue(1);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), (1 + 1) / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 4 / 4.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 4);

        stats.AddValue(3);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), 1);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 7 / 5.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 5);

        stats.AddValue(5);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), (1 + 1) / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 12 / 6.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 6);

        stats.AddValue(7);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), 1);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 19 / 7.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 7);

        stats.AddValue(7);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), (1 + 3) / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 26 / 8.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 8);

        stats.AddValue(8);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), 3);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 34 / 9.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 9);

        stats.AddValue(100);
        UNIT_ASSERT_EQUAL(stats.GetMedian(), (3 + 5) / 2.0);
        UNIT_ASSERT_EQUAL(stats.GetMean(), 134 / 10.0);
        UNIT_ASSERT_EQUAL(stats.GetN(), 10);
    }

}

}
}
