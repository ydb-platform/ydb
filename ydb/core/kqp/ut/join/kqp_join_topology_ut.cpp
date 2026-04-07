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

        auto numTablesRanged = args.GetArg<ui64>("N");

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

                                        Cout << "state=" << state << "'\n";

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

        auto kikimr = GetCBOTestsYDB(TString(customQuery.Stats), TDuration::Seconds(10));
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
