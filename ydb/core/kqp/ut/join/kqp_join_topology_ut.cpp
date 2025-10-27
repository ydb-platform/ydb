#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_benches.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

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
        std::string queryWithShuffleElimination = "PRAGMA ydb.OptShuffleElimination = \"";
        queryWithShuffleElimination += enableShuffleElimination ? "true" : "false";
        queryWithShuffleElimination += "\";\n";
        queryWithShuffleElimination += "PRAGMA ydb.MaxDPHypDPTableSize='4294967295';\n";
        queryWithShuffleElimination += "PRAGMA ydb.ForceShuffleElimination='true';\n";
        queryWithShuffleElimination += "PRAGMA ydb.CostBasedOptimizationLevel = \"" + std::to_string(optLevel) + "\";\n";
        queryWithShuffleElimination += query;

        return queryWithShuffleElimination;
    }

    std::optional<TStatistics<ui64>> BenchmarkExplain(TBenchmarkConfig config, NYdb::NQuery::TSession session, const TString& query) {
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

        stats->Dump(Cout);

        return stats;
    }


    std::optional<TStatistics<ui64>> BenchmarkShuffleElimination(TBenchmarkConfig config, NYdb::NQuery::TSession session, const TString& query) {
        Cout << "--------------------------------- W/O CBO --------------------------------\n";
        BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/1));
        Cout << "--------------------------------- CBO-SE ---------------------------------\n";
        BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/false, /*optLevel=*/2));
        Cout << "--------------------------------- CBO+SE ---------------------------------\n";
        auto result = BenchmarkExplain(config, session, ConfigureQuery(query, /*enableShuffleElimination=*/true,  /*optLevel=*/2));
        Cout << "--------------------------------------------------------------------------\n";

        return result;
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

    bool BenchmarkShuffleEliminationOnTopology(TBenchmarkConfig config, NYdb::NQuery::TSession session, TRelationGraph graph) {
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
            return false;
        }

        Cout << "================================= BENCHMARK ==============================\n";
        TString query = graph.MakeQuery();
        Cout << query;
        auto resultTime = BenchmarkShuffleElimination(config, session, query);

        Cout << "================================= FINALIZE ===============================\n";
        auto deletionQuery = graph.GetSchema().MakeDropQuery();
        Cout << deletionQuery;
        if (!ExecuteQuery(session, deletionQuery)) { // TODO: this is really bad probably?
            return false;
        }
        Cout << "==========================================================================\n";

        if (!resultTime) {
            return false; // query timed out
        }

        return true;
    }

    template <auto Lambda>
    void BenchmarkShuffleEliminationOnTopologies(int maxNumTables, double probabilityStep, unsigned seed = 0) {
        auto config = TBenchmarkConfig {
            .Warmup = {
                .MinRepeats = 1,
                .MaxRepeats = 3,
                .Timeout = 10'000'000,
            },

            .Bench = {
                .MinRepeats = 3,
                .MaxRepeats = 100,
                .Timeout = 1'000'000'000,
            },
        };

        std::mt19937 mt(seed);

        TSchema fullSchema = TSchema::MakeWithEnoughColumns(maxNumTables);
        TString stats = TSchemaStats::MakeRandom(mt, fullSchema, 7, 10).ToJSON();

        auto kikimr = GetCBOTestsYDB(stats, TDuration::Seconds(10));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        for (int i = 2 /* one table is not possible with all topologies, so 2 */; i < maxNumTables; i ++) {
            for (double probability = 0; probability <= 1.0; probability += probabilityStep) {
                TRelationGraph graph = Lambda(mt, i, probability);
                BenchmarkShuffleEliminationOnTopology(config, session, graph);
            }
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

    Y_UNIT_TEST(ShuffleEliminationBenchmarkOnStar) {
        BenchmarkShuffleEliminationOnTopologies<GenerateStar>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    }

    Y_UNIT_TEST(ShuffleEliminationBenchmarkOnLine) {
        BenchmarkShuffleEliminationOnTopologies<GenerateLine>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    }

    Y_UNIT_TEST(ShuffleEliminationBenchmarkOnFullyConnected) {
        BenchmarkShuffleEliminationOnTopologies<GenerateFullyConnected>(/*maxNumTables=*/15, /*probabilityStep=*/0.2);
    }

    Y_UNIT_TEST(ShuffleEliminationBenchmarkOnRandomTree) {
        BenchmarkShuffleEliminationOnTopologies<GenerateRandomTree>(/*maxNumTables=*/25, /*probabilityStep=*/0.2);
    }

}

}
}
