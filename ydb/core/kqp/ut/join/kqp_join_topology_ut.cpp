#include <cstdint>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_benches.h>
#include <ydb/core/kqp/ut/common/kqp_arg_parser.h>

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
        queryWithShuffleElimination += "PRAGMA ydb.ForceShuffleElimination='true';\n";
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

    TKikimrRunner GetCBOTestsYDB(TString stats, TDuration compilationTimeout) {
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

        return TKikimrRunner(serverSettings);
    }

    std::optional<TRunningStatistics<double>> BenchmarkShuffleEliminationOnTopology(TBenchmarkConfig config, NYdb::NQuery::TSession session, TRelationGraph graph) {
        Cout << "\n\n";
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

    template <auto Lambda>
    void BenchmarkShuffleEliminationOnTopologies(TBenchmarkConfig config, TArgs::TRangedValue<ui64> numTables, TArgs::TRangedValue<double> sameColumnProbability, unsigned seed = 0) {
        std::mt19937 mt(seed);

        TSchema fullSchema = TSchema::MakeWithEnoughColumns(numTables.GetLast());
        TString stats = TSchemaStats::MakeRandom(mt, fullSchema, 7, 10).ToJSON();

        auto kikimr = GetCBOTestsYDB(stats, TDuration::Seconds(10));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto OS = TUnbufferedFileOutput("results.csv");
        DumpBoxPlotCSVHeader(OS);

        for (ui64 n : numTables) {
            for (double probability : sameColumnProbability) {
                TRelationGraph graph = Lambda(mt, n, probability);
                auto result = BenchmarkShuffleEliminationOnTopology(config, session, graph);
                if (!result) {
                    goto stop;
                }

                DumpBoxPlotToCSV(OS, n, result->GetStatistics());
            }
        }
    stop:;
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

    // Y_UNIT_TEST(ShuffleEliminationBenchmarkOnStar) {
    //     BenchmarkShuffleEliminationOnTopologies<GenerateStar>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    // }

    // Y_UNIT_TEST(ShuffleEliminationBenchmarkOnLine) {
    //     BenchmarkShuffleEliminationOnTopologies<GenerateLine>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    // }

    // Y_UNIT_TEST(ShuffleEliminationBenchmarkOnFullyConnected) {
    //     BenchmarkShuffleEliminationOnTopologies<GenerateFullyConnected>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    // }

    // Y_UNIT_TEST(ShuffleEliminationBenchmarkOnRandomTree) {
    //     BenchmarkShuffleEliminationOnTopologies<GenerateRandomTree>(/*maxNumTables=*/25, /*probabilityStep=*/0.2);
    // }

    Y_UNIT_TEST(Benchmark) {
        TArgs args{GetTestParam("TOPOLOGY")};

        auto config = GetBenchmarkConfig(args);
        DumpBenchmarkConfig(Cout, config);

        BenchmarkShuffleEliminationOnTopologies<GenerateRandomTree>(
            config,
            /*maxNumTables=*/args.GetArg<uint64_t>("N"),
            /*probabilityStep=*/args.GetArg<double>("P"));
    }

}

}
}
