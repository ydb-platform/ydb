#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/string/printf.h>

#include <algorithm>
#include <fstream>
#include <regex>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

TString GetStatic(const TString& filePath) {
    TString fullPath = SRC_("data/" + filePath);

    std::ifstream file(fullPath);

    if (!file.is_open()) {
        throw std::runtime_error("can't open + " + fullPath + " " + std::filesystem::current_path());
    }

    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}

void CreateTables(NYdb::NQuery::TSession session, const TString& schemaPath, bool useColumnStore) {
    std::string query = GetStatic(schemaPath);

    if (useColumnStore) {
        std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
        query = std::regex_replace(query, pattern, "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    }

    auto res = session.ExecuteQuery(TString(query), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    res.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(res.IsSuccess());
}

void CreateView(NYdb::NQuery::TSession session, const TString& viewPath) {
    std::string query = GetStatic(viewPath);
    auto res = session.ExecuteQuery(TString(query), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    res.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(res.IsSuccess());
}

TString GetPrettyJSON(const NJson::TJsonValue& json) {
    TStringStream ss;
    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);
    writer.WriteJsonValue(&json);
    writer.FlushTo(&ss); ss << Endl;
    return ss.Str();
}

/*
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
 */
static void CreateSampleTable(NYdb::NQuery::TSession session, bool useColumnStore) {
    CreateTables(session, "schema/rstuv.sql", useColumnStore);

    CreateTables(session, "schema/tpch.sql", useColumnStore);

    CreateTables(session, "schema/tpcds.sql", useColumnStore);

    CreateTables(session, "schema/tpcc.sql", useColumnStore);

    CreateTables(session, "schema/lookupbug.sql", useColumnStore);

    CreateTables(session, "schema/sortings.sql", useColumnStore);

    {
        CreateTables(session, "schema/different_join_predicate_key_types.sql", false /* olap params are already set in schema */);
        const TString upsert =
        R"(
            UPSERT INTO t1 (id1) VALUES (1);
            UPSERT INTO t2 (id2, t1_id1) VALUES (1, 1);
            UPSERT INTO t3 (id3) VALUES (1);
        )";
        auto result =
            session.ExecuteQuery(
                upsert,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    CreateView(session, "view/tpch_random_join_view.sql");
}

struct TExecuteParams {
    bool RemoveLimitOperator = false;
    bool EnableSeparationComputeActorsFromRead = true;
};

static TKikimrRunner GetKikimrWithJoinSettings(
    bool useStreamLookupJoin = false,
    TString stats = "",
    bool useCBO = true,
    const TExecuteParams& params = {}
){
    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;

    if (stats != "") {
        setting.SetName("OptOverrideStatistics");
        setting.SetValue(stats);
        settings.push_back(setting);
    }

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(useStreamLookupJoin);
    appConfig.MutableTableServiceConfig()->SetEnableOrderOptimizaionFSM(true);
    appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());
    if (!useCBO) {
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
    } else {
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
    }
    TKikimrSettings serverSettings(appConfig);
    serverSettings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(params.EnableSeparationComputeActorsFromRead);
    serverSettings.SetKqpSettings(settings);

    serverSettings.SetNodeCount(1);
    #if defined(_asan_enabled_)
        serverSettings.SetNodeCount(1);
    #endif

    serverSettings.WithSampleTables = false;

    return TKikimrRunner(serverSettings);
}

void Replace(std::string& s, const std::string& from, const std::string& to) {
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
        s.replace(pos, from.size(), to);
        pos += to.size();
    }
}

void PrintPlan(const TString& plan) {
    Cout << plan << Endl;
    NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, true, Cout, 0);
    queryPlanPrinter.Print(plan);

    std::string joinOrder = GetJoinOrder(plan).GetStringRobust();

    Cout << "JoinOrder: " << joinOrder << Endl;
    std::replace(joinOrder.begin(), joinOrder.end(), '[', '(');
    std::replace(joinOrder.begin(), joinOrder.end(), ']', ')');
    std::replace(joinOrder.begin(), joinOrder.end(), ',', ' ');
    joinOrder.erase(std::remove(joinOrder.begin(), joinOrder.end(), '\"'), joinOrder.end());
    joinOrder.erase(std::remove(joinOrder.begin(), joinOrder.end(), '\\'), joinOrder.end());


    size_t pos;
    std::string tpcdsTablePrefix = "test/ds/";
    while ((pos = joinOrder.find(tpcdsTablePrefix)) != std::string::npos) {
        joinOrder.erase(pos, tpcdsTablePrefix.length());
    }

    Cout << "JoinOrder" << joinOrder << Endl;
}

class TFindJoinWithLabels {
public:
    TFindJoinWithLabels(
        const NJson::TJsonValue& fullPlan
    )
        : Plan(
            GetDetailedJoinOrder(
                fullPlan.GetStringRobust(),
                TGetPlanParams{
                    .IncludeFilters = false,
                    .IncludeOptimizerEstimation = false,
                    .IncludeTables = true,
                    .IncludeShuffles = true
                }
            )
        )
    {}

    struct TJoin {
        TString Join;
        bool LhsShuffled;
        bool RhsShuffled;
    };

    enum ESearchSettings : ui32 {
        ExactMatch = 0, // We search join tree with full exact match of labels
        PartialMatch = 1 // We search the first join tree, which labels overlap provided.
    };

    TJoin Find(const TVector<TString>& labels, ESearchSettings settings = ExactMatch) {
        RequestedLabels = labels;
        Settings = settings;

        std::sort(RequestedLabels.begin(), RequestedLabels.end());
        TVector<TString> dummy;
        auto res = FindImpl(Plan, dummy);
        UNIT_ASSERT_C(!res.Join.empty(), "Join wasn't found.");
        return res;
    }

private:
    TJoin FindImpl(const NJson::TJsonValue& plan, TVector<TString>& subtreeLabels) {
        auto planMap = plan.GetMapSafe();
        if (!planMap.contains("table")) {
            TString opName = planMap.at("op_name").GetStringSafe();

            auto inputs = planMap.at("args").GetArraySafe();
            for (size_t i = 0; i < inputs.size(); ++i) {
                TVector<TString> childLabels;
                auto maybeJoin = FindImpl(inputs[i], childLabels);
                if (!maybeJoin.Join.empty()) {
                    return maybeJoin;
                }
                subtreeLabels.insert(subtreeLabels.end(), childLabels.begin(), childLabels.end());
            }

            if (AreRequestedLabels(subtreeLabels)) {
                TString lhsInput = inputs[0].GetMapSafe()["op_name"].GetStringSafe();
                TString rhsInput = inputs[1].GetMapSafe()["op_name"].GetStringSafe();
                return {opName, lhsInput.find("HashShuffle") != TString::npos, rhsInput.find("HashShuffle") != TString::npos};
            }

            return TJoin{};
        }

        subtreeLabels = {planMap.at("table").GetStringSafe()};
        return TJoin{};
    }

    bool AreRequestedLabels(TVector<TString> labels) {
        switch (Settings) {
            case ExactMatch: {
                std::sort(labels.begin(), labels.end());
                return RequestedLabels == labels;
            }
            case PartialMatch: {
                if (labels.size() < RequestedLabels.size()) {
                    return false;
                }

                for (const auto& requestedLabel: RequestedLabels) {
                    if (std::find(labels.begin(), labels.end(), requestedLabel) == labels.end()) {
                        return false;
                    }
                }
                return true;
            }
            default: {
                Y_ENSURE(false, "No such setting.");
            }
        }
    }

    ESearchSettings Settings;
    NJson::TJsonValue Plan;
    TVector<TString> RequestedLabels;
};


class TBenchMarkInvariantsChecker {
public:
    enum EBenchmark : std::uint32_t {
        Undefined = 0,
        TPCH = 1,
    };

    void Check(const TString& queryPath, const TString& fullPlan) {
        EBenchmark bench = GetBenchmarkByQueryPath(queryPath);
        switch (bench) {
            case TPCH: {
                CheckTPCH(fullPlan);
            }
            default: {
                return;
            }
        }
    }

private:
    EBenchmark GetBenchmarkByQueryPath(const TString& queryPath) {
        if (queryPath.find("tpch") != TString::npos) {
            return EBenchmark::TPCH;
        }

        return EBenchmark::Undefined;
    }

    void CheckTPCH(const TString& fullPlan) {
        TFindJoinWithLabels joinFinder(fullPlan);

        if (fullPlan.find("nation") != TString::npos) {
            auto join = joinFinder.Find({"nation"}, TFindJoinWithLabels::PartialMatch);
            AssertLookupOrMapJoin(join.Join);
        }

        if (fullPlan.find("region") != TString::npos) {
            auto join = joinFinder.Find({"region"}, TFindJoinWithLabels::PartialMatch);
            AssertLookupOrMapJoin(join.Join);
        }
    }

    void AssertLookupOrMapJoin(const TString& join) {
        std::string joinLower{join.begin(), join.end()};
        std::transform(joinLower.begin(), joinLower.end(), joinLower.begin(), ::tolower);

        bool containsLookupOrMap =
            (joinLower.find("lookup") != std::string::npos) ||
            (joinLower.find("map") != std::string::npos);

        UNIT_ASSERT_C(containsLookupOrMap, TStringBuilder{} << joinLower << " isn't map or lookup join, but expected to be!");
    }
};

class TChainTester {
public:
    TChainTester(size_t chainSize)
        : Kikimr(GetKikimrWithJoinSettings(false, GetStats(chainSize)))
        , TableClient(Kikimr.GetTableClient())
        , Session(TableClient.GetSession().GetValueSync().GetSession())
        , ChainSize(chainSize)
    {}

public:
    void Test() {
        CreateTables();
        JoinTables();
    }

    static TString GetStats(size_t chainSize) {
        srand(228);
        NJson::TJsonValue stats;
        for (size_t i = 0; i < chainSize; ++i) {
            ui64 nRows = rand();
            NJson::TJsonValue tableStat;
            tableStat["n_rows"] = nRows;
            tableStat["byte_size"] = nRows * 10;

            TString table = Sprintf("/Root/table_%ld", i);
            stats[table] = std::move(tableStat);
        }
        return stats.GetStringRobust();
    }

private:
    void CreateTables() {
        for (size_t i = 0; i < ChainSize; ++i) {
            TString tableName = Sprintf("/Root/table_%ld", i);

            TString createTable = Sprintf(
                "CREATE TABLE `%s` (id%ld Int32, PRIMARY KEY (id%ld));",
                tableName.c_str(), i, i
            );

            auto result = Session.ExecuteSchemeQuery(createTable).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    void JoinTables() {
        TString joinRequest;

        joinRequest.append("SELECT * FROM `/Root/table_0` as t0 ");

        for (size_t i = 1; i < ChainSize; ++i) {
            TString table = Sprintf("/Root/table_%ld", i);

            TString prevAliasTable = Sprintf("t%ld", i - 1);
            TString aliasTable = Sprintf("t%ld", i);

            joinRequest +=
                Sprintf(
                    "INNER JOIN `%s` AS %s ON %s.id%ld = %s.id%ld ",
                    table.c_str(), aliasTable.c_str(), aliasTable.c_str(), i, prevAliasTable.c_str(), i - 1
                );
        }

        auto result = Session.ExplainDataQuery(joinRequest).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        PrintPlan(TString{result.GetPlan()});
    }

    TKikimrRunner Kikimr;
    NYdb::NTable::TTableClient TableClient;
    TSession Session;
    size_t ChainSize;
};

void ExplainJoinOrderTestDataQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore, bool useCBO = true) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), useCBO);
    auto db = kikimr.GetQueryClient();
    auto result = db.GetSession().GetValueSync();
    NStatusHelpers::ThrowOnError(result);
    auto session = result.GetSession();


    CreateSampleTable(session, useColumnStore);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);

        auto result =
            session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        PrintPlan(TString{*result.GetStats()->GetPlan()});
    }
}

void TestOlapEstimationRowsCorrectness(const TString& queryPath, const TString& statsPath) {
    auto kikimr = GetKikimrWithJoinSettings(false, GetStatic(statsPath));
    auto db = kikimr.GetQueryClient();
    auto result = db.GetSession().GetValueSync();
    NStatusHelpers::ThrowOnError(result);
    auto session = result.GetSession();


    CreateSampleTable(session, true);

    const TString actualQuery = GetStatic(queryPath);
    TString actualPlan;
    {
        auto result =
            session.ExecuteQuery(
                actualQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        actualPlan = *result.GetStats()->GetPlan();
        PrintPlan(actualPlan);
        Cout << result.GetStats()->GetAst() << Endl;
    }

    const TString expectedQuery = R"(PRAGMA kikimr.OptEnableOlapPushdown = "false";)" "\n" + actualQuery;
    TString expectedPlan;
    {
        auto result = session.ExecuteQuery(
                expectedQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        expectedPlan = *result.GetStats()->GetPlan();
        PrintPlan(expectedPlan);
        Cout << result.GetStats()->GetAst() << Endl;
    }

    auto expectedDetailedPlan = GetDetailedJoinOrder(actualPlan, {.IncludeFilters = true, .IncludeOptimizerEstimation = true, .IncludeTables = false});
    auto actualDetailedPlan =  GetDetailedJoinOrder(expectedPlan, {.IncludeFilters = true, .IncludeOptimizerEstimation = true, .IncludeTables = false});
    Cout << expectedDetailedPlan << Endl;
    Cout << actualDetailedPlan << Endl;
    UNIT_ASSERT_VALUES_EQUAL(expectedDetailedPlan, actualDetailedPlan);
}

/* Tests to check olap selectivity correctness */
Y_UNIT_TEST_SUITE(OlapEstimationRowsCorrectness) {
    Y_UNIT_TEST(TPCH2) {
        TestOlapEstimationRowsCorrectness("queries/tpch2.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH3) {
       TestOlapEstimationRowsCorrectness("queries/tpch3.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH5) {
        TestOlapEstimationRowsCorrectness("queries/tpch5.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH9) {
        TestOlapEstimationRowsCorrectness("queries/tpch9.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH10) {
        TestOlapEstimationRowsCorrectness("queries/tpch10.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH11) {
        TestOlapEstimationRowsCorrectness("queries/tpch11.sql", "stats/tpch1000s.json");
    }

    Y_UNIT_TEST(TPCH21) {
        TestOlapEstimationRowsCorrectness("queries/tpch21.sql", "stats/tpch1000s.json");
    }

    // Y_UNIT_TEST(TPCDS16) { // filter under filter (filter -> filter -> table) maybe constant folding doesn't work
    //     TestOlapEstimationRowsCorrectness("queries/tpcds16.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS34) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds34.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS61) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds61.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS64) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds64.sql", "stats/tpcds1000s.json");
    // }

    Y_UNIT_TEST(TPCDS78) {
        TestOlapEstimationRowsCorrectness("queries/tpcds78.sql", "stats/tpcds1000s.json");
    }

    // Y_UNIT_TEST(TPCDS87) {
    //     TestOlapEstimationRowsCorrectness("queries/tpcds87.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS88) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds88.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS90) { // ??? <---- filter olap + / oltp -
    //     TestOlapEstimationRowsCorrectness("queries/tpcds90.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS92) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds92.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS94) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds94.sql", "stats/tpcds1000s.json");
    // }

    // Y_UNIT_TEST(TPCDS95) { // ???
    //     TestOlapEstimationRowsCorrectness("queries/tpcds95.sql", "stats/tpcds1000s.json");
    // }

    Y_UNIT_TEST(TPCDS96) {
        TestOlapEstimationRowsCorrectness("queries/tpcds96.sql", "stats/tpcds1000s.json");
    }
}

Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(Chain65Nodes) {
        TChainTester(65).Test();
    }

    std::pair<TString, std::vector<NYdb::TResultSet>> ExecuteJoinOrderTestGenericQueryWithStats(
        const TString& queryPath,
        const TString& statsPath,
        bool useStreamLookupJoin,
        bool useColumnStore,
        bool useCBO = true,
        const TExecuteParams& params = {}
    ) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), useCBO, params);
        auto db = kikimr.GetQueryClient();
        auto result = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session = result.GetSession();


        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            std::string query = GetStatic(queryPath);
            if (params.RemoveLimitOperator) {
                std::regex pattern(R"(\blimit\s+\d+\b)", std::regex_constants::icase);
                query = std::regex_replace(std::string{query}, pattern, "");
            }
            Cout << query << Endl;

            auto explainRes = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            explainRes.GetIssues().PrintTo(Cerr);
            for (const auto& issue: explainRes.GetIssues()) {
                for (const auto& subissue: issue.GetSubIssues()) {
                    UNIT_ASSERT_C(!(8000 <= subissue->IssueCode && subissue->IssueCode < 9000), "CBO didn't work for this query!");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(explainRes.GetStatus(), EStatus::SUCCESS);
            TString plan = *explainRes.GetStats()->GetPlan();
            PrintPlan(plan);

            TBenchMarkInvariantsChecker().Check(queryPath, plan);

            auto execRes = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            execRes.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(execRes.GetStatus(), EStatus::SUCCESS);

            return {plan, execRes.GetResultSets()};
        }
    }

    void CheckJoinCardinality(const TString& queryPath, const TString& statsPath, const TString& joinKind, double card, bool useStreamLookupJoin, bool useColumnStore) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), true);
        auto db = kikimr.GetQueryClient();
        auto result = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session = result.GetSession();


        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);

            auto result =
                session.ExecuteQuery(
                    query,
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();
            PrintPlan(TString{*result.GetStats()->GetPlan()});
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);

            if(!useStreamLookupJoin) {
                auto joinNode = FindPlanNodeByKv(plan.GetMapSafe().at("SimplifiedPlan"), "Node Type", joinKind);
                UNIT_ASSERT(joinNode.IsDefined());
                auto op = joinNode.GetMapSafe().at("Operators").GetArraySafe()[0];
                auto eRows = op.GetMapSafe().at("E-Rows").GetStringSafe();
                UNIT_ASSERT_EQUAL(std::stod(eRows), card);
            }
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinStatsOverride, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_stats_override.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FourWayJoinLeftFirst, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/four_way_join_left_first.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithPreds, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_preds.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_complex_preds.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds2, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_complex_preds2.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithPredsAndEquiv, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/four_way_join_with_preds_and_equiv.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FourWayJoinWithPredsAndEquivAndLeft, ColumnStore) {
       ExecuteJoinOrderTestGenericQueryWithStats(
        "queries/four_way_join_with_preds_and_equiv_and_left.sql", "stats/basic.json", false, ColumnStore
        );
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFold, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/five_way_join_with_constant_fold.sql", "stats/basic.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFoldOpt, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/five_way_join_with_constant_fold_opt.sql", "stats/basic.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(DatetimeConstantFold, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/datetime_constant_fold.sql", "stats/basic.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(UdfConstantFold, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/udf_constant_fold.sql", "stats/basic.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCHRandomJoinViewJustWorks, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch_random_join_view_just_works.sql", "stats/tpch1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS16, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds16.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    /* tpcds23 has > 1 result sets */
    Y_UNIT_TEST(TPCDS23) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/tpcds23.sql", "stats/tpcds1000s.json", false, true
        );
    }

    bool CheckNoSortings(const TString& plan) {
        return !plan.Contains("Top") && !plan.Contains("SortBy");
    }

    bool CheckSorting(const TString& plan) {
        return !CheckNoSortings(plan);
    }

    // tests, that check redudant sortings removal : if RemoveLimit is on we delete limit from the query to check
    // if a sort operator was deleted, otherwise we want topsort deleted
    Y_UNIT_TEST_TWIN(SortingsSimpleOrderByPKAlias, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_simple_order_by_pk_alias.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsSimpleOrderByAliasIndexDesc, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_simple_order_by_alias_index_desc.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsWithLookupJoin1, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_with_lookupjoin_1.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsWithLookupJoin2, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_with_lookupjoin_2.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsWithLookupJoin3, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_with_lookupjoin_3.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsWithLookupJoin4, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_with_lookupjoin_4.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsByPrefixWithAttrEquiToPK, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_by_prefix_with_attr_equiv_to_pk.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsByPKWithLookupJoin, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_by_pk_with_lookupjoin.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsWithLookupJoinByPrefix, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_with_lookupjoin_by_prefix.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsByPrefixWithConstant, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_by_prefix_with_constant.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsByPK, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_by_pk.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(Sortings4Year, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_4_year.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsComplexOrderBy, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_complex_order_by.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsPropagateThroughMapJoin, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_propagate_through_map_join.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckNoSortings(plan));
    }

    Y_UNIT_TEST_TWIN(SortingsDifferentDirs, RemoveLimitOperator) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/sortings_different_dirs.sql", "stats/sortings.json", true, false, true, {.RemoveLimitOperator = RemoveLimitOperator});
        UNIT_ASSERT(CheckSorting(plan));
    }

    Y_UNIT_TEST_TWIN(TPCDS34, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds34.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS61, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds61.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS87, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds87.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS88, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds88.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS90, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds90.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS92, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds92.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS94, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds94.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS95, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds95.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS96, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds96.sql", "stats/tpcds1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TestJoinHint1, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint1.sql", "stats/basic.json", "InnerJoin (Grace)", 10e6, false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TestJoinHint2, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint2.sql", "stats/basic.json", "InnerJoin (Map)", 1, false, ColumnStore);
    }

    Y_UNIT_TEST(BytesHintForceGraceJoin) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/bytes_hint_force_grace_join.sql", "stats/basic.json", false, true, true);
        auto joinFinder = TFindJoinWithLabels(plan);
        auto join = joinFinder.Find({"R", "S"});
        UNIT_ASSERT_C(join.Join == "InnerJoin (Grace)", join.Join);
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationOneJoin, EnableSeparationComputeActorsFromRead) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/shuffle_elimination_one_join.sql", "stats/tpch1000s.json", false, true, true, {.EnableSeparationComputeActorsFromRead = EnableSeparationComputeActorsFromRead});
        auto joinFinder = TFindJoinWithLabels(plan);
        auto join = joinFinder.Find({"customer", "orders"});
        UNIT_ASSERT_C(join.Join == "InnerJoin (Grace)", join.Join);
        UNIT_ASSERT(!join.LhsShuffled);
        UNIT_ASSERT(join.RhsShuffled);
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationReuseShuffleTwoJoins, EnableSeparationComputeActorsFromRead) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/shuffle_elimination_reuse_shuffle_two_joins.sql", "stats/tpch1000s.json", false, true, true, {.EnableSeparationComputeActorsFromRead = EnableSeparationComputeActorsFromRead});
        auto joinFinder = TFindJoinWithLabels(plan);

        {
            auto join = joinFinder.Find({"partsupp", "part"});
            UNIT_ASSERT_C(join.Join == "InnerJoin (Grace)", join.Join);
            UNIT_ASSERT(join.LhsShuffled);
            UNIT_ASSERT(!join.RhsShuffled);
        }

        {
            auto join = joinFinder.Find({"partsupp", "part", "supplier"});
            UNIT_ASSERT_C(join.Join == "InnerJoin (Grace)", join.Join);
            UNIT_ASSERT(!join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationDifferentJoinPredicateKeyTypeCorrectness1, EnableSeparationComputeActorsFromRead) {
        auto [plan, resultSets] = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_different_join_predicate_key_type_correctness1.sql", "stats/different_join_predicate_key_types.json", false, false, true, {.EnableSeparationComputeActorsFromRead=EnableSeparationComputeActorsFromRead}
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        auto join = joinFinder.Find({"t1", "t2"});
        UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Grace)");
        UNIT_ASSERT(!join.LhsShuffled);
        UNIT_ASSERT(join.RhsShuffled);

        UNIT_ASSERT(resultSets.size() == 1);
        auto resultSet = FormatResultSetYson(resultSets[0]);
        UNIT_ASSERT_EQUAL_C(resultSet, "[[1;1;1]]", resultSet);
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationDifferentJoinPredicateKeyTypeCorrectness2, EnableSeparationComputeActorsFromRead) {
        auto [plan, resultSets] = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_different_join_predicate_key_type_correctness2.sql", "stats/different_join_predicate_key_types.json", false, false, true, {.EnableSeparationComputeActorsFromRead = EnableSeparationComputeActorsFromRead}
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        {
            auto join = joinFinder.Find({"t1", "t2"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Grace)");
            UNIT_ASSERT(!join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }

        {
            auto join = joinFinder.Find({"t1", "t2", "t3"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Grace)");
            UNIT_ASSERT(join.LhsShuffled);
            UNIT_ASSERT(!join.RhsShuffled);
        }

        UNIT_ASSERT(resultSets.size() == 1);
        auto resultSet = FormatResultSetYson(resultSets[0]);
        UNIT_ASSERT_EQUAL_C(resultSet, "[[1;1;1;1]]", resultSet);
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationManyKeysJoinPredicate, EnableSeparationComputeActorsFromRead) {
        auto [plan, resultSets] = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_many_keys_join_predicate.sql", "stats/tpch1000s.json", false, true, true, {.EnableSeparationComputeActorsFromRead = EnableSeparationComputeActorsFromRead}
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        {
            auto join = joinFinder.Find({"partsupp", "lineitem"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Grace)");
            UNIT_ASSERT(!join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }
    }

    Y_UNIT_TEST_TWIN(ShuffleEliminationTpcdsMapJoinBug, EnableSeparationComputeActorsFromRead) {
        auto [plan, resultSets] = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_tpcds_map_join_bug.sql", "stats/tpcds1000s.json", false, true, true, {.EnableSeparationComputeActorsFromRead = EnableSeparationComputeActorsFromRead}
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        {
            auto join = joinFinder.Find({"customer", "customer_address"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Map)");
        }
        {
            auto join = joinFinder.Find({"customer_demographics", "customer", "customer_address"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Map)");
        }
        {
            auto join = joinFinder.Find({"customer_demographics", "customer", "customer_address", "store_sales"});
            UNIT_ASSERT_EQUAL(join.Join, "LeftSemiJoin (Grace)");
            UNIT_ASSERT(join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }
    }

    Y_UNIT_TEST(TPCH12_100) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch12.sql", "stats/tpch100s.json", false, true, true);
    }


    Y_UNIT_TEST(TPCH9_100) {
        auto [plan, _] =  ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch9.sql", "stats/tpch100s.json", false, true);
        auto joinFinder = TFindJoinWithLabels(plan);
        auto join = joinFinder.Find({"nation"}, TFindJoinWithLabels::PartialMatch);
        UNIT_ASSERT_C(join.Join == "InnerJoin (Map)", join.Join);
    }

    Y_UNIT_TEST(OltpJoinTypeHintCBOTurnOFF) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/oltp_join_type_hint_cbo_turnoff.sql", "stats/basic.json", false, false, false);
        auto joinFinder = TFindJoinWithLabels(plan);
        UNIT_ASSERT(joinFinder.Find({"R", "S"}).Join == "InnerJoin (Grace)");
        UNIT_ASSERT(joinFinder.Find({"R", "S", "T"}).Join == "InnerJoin (Map)");
        UNIT_ASSERT(joinFinder.Find({"R", "S", "T", "U"}).Join == "InnerJoin (Grace)");
        UNIT_ASSERT(joinFinder.Find({"R", "S", "T", "U", "V"}).Join == "InnerJoin (Map)");
    }

    Y_UNIT_TEST_TWIN(TestJoinOrderHintsSimple, ColumnStore) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_simple.sql", "stats/basic.json", false, ColumnStore);
        UNIT_ASSERT_VALUES_EQUAL(GetJoinOrder(plan).GetStringRobust(), R"(["T",["R","S"]])") ;
    }

    Y_UNIT_TEST_TWIN(TestJoinOrderHintsComplex, ColumnStore) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_complex.sql", "stats/basic.json", false, ColumnStore);
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"([["R","S"],["T","U"]])") != TString::npos, joinOrder);
    }

    Y_UNIT_TEST(TestJoinOrderHintsManyHintTrees) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_many_hint_trees.sql", "stats/basic.json", false, true);
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"(["R","S"])") != TString::npos, joinOrder);
        UNIT_ASSERT_C(joinOrder.find(R"(["T","U"])") != TString::npos, joinOrder);
    }

    void CanonizedJoinOrderTest(
        const TString& queryPath, const TString& statsPath, TString correctJoinOrderPath, bool useStreamLookupJoin, bool useColumnStore
    ) {
        auto [plan, _] = ExecuteJoinOrderTestGenericQueryWithStats(queryPath, statsPath, useStreamLookupJoin, useColumnStore);

        if (useStreamLookupJoin) {
            return;
        }

        if (useColumnStore) {
            correctJoinOrderPath = correctJoinOrderPath.substr(0, correctJoinOrderPath.find(".json")) + "_column_store.json";
        }

        auto currentJoinOrder = GetPrettyJSON(GetDetailedJoinOrder(plan, TGetPlanParams{.IncludeShuffles = true})).append("\n");

        /* to canonize the tests use --test-param CANONIZE_JOIN_ORDER_TESTS=TRUE */
        TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS"); canonize.to_lower();
        if (canonize.equal("true")) {
            Cerr << "--------------------CANONIZING THE TESTS--------------------";
            TOFStream stream(SRC_("data/" + correctJoinOrderPath));
            stream << currentJoinOrder;
            stream.Finish();
        }

        TString ref = GetStatic(correctJoinOrderPath);
        Cout << "actual\n" << GetJoinOrder(plan).GetStringRobust() << Endl;
        Cout << "expected\n" << GetJoinOrderFromDetailedJoinOrder(ref).GetStringRobust() << Endl;
        UNIT_ASSERT(currentJoinOrder == ref);
    }

    void RunBenchmarkQueries(
        NYdb::NQuery::TSession& session,
        const std::string& benchmarkName,
        const std::string& constsPath,
        const std::vector<std::string>& queryTypes,
        size_t queryCount,
        const std::string& queryPathTemplate,
        const std::string& tablePrefix
    ) {

        std::string consts;
        if (!constsPath.empty()) {
            TIFStream s(constsPath);
            consts = s.ReadAll();
        }

        for (const std::string& queryType : queryTypes) {
            for (std::size_t i = 1; i <= queryCount; ++i) {
                TString qPath = TStringBuilder{} << ArcadiaSourceRoot() << queryPathTemplate << queryType << "/" << "q" << i << ".sql";

                TIFStream s(qPath);
                std::string q = s.ReadAll();
                Replace(q, "{path}", tablePrefix);
                Replace(q, "{% include 'header.sql.jinja' %}", "");
                std::regex pattern(R"(\{\{\s*([a-zA-Z0-9_]+)\s*\}\})");
                q = std::regex_replace(q, pattern, "`" + tablePrefix + "$1`");
                if (queryType == "yql" && !consts.empty()) {
                    q = consts + "\n" + q;
                }

                Cout << "Running " << benchmarkName << " query: " << i << ", type: " << queryType << Endl;
                auto settings = NYdb::NQuery::TExecuteQuerySettings();
                auto result = session.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder{} << "query " << benchmarkName << "#" << i
                    << ", type: " << queryType << " failed, query:\n" << q);
            }
        }
    }

    Y_UNIT_TEST_TWIN(TPCHEveryQueryWorks, ColumnStore) {
        auto kikimr = GetKikimrWithJoinSettings(false, GetStatic("stats/tpch1000s.json"), true);
        auto db = kikimr.GetQueryClient();
        auto result = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session = result.GetSession();

        CreateTables(session, "schema/tpch.sql", ColumnStore);

        RunBenchmarkQueries(
            session,
            "TPCH",
            ArcadiaSourceRoot() + "/ydb/library/benchmarks/gen_queries/consts.yql",
            {"yql", "nice", "ydb"},
            22,
            "/ydb/library/benchmarks/queries/tpch/",
            "/Root/"
        );
    }

    Y_UNIT_TEST(TPCDSEveryQueryWorks) {
        auto kikimr = GetKikimrWithJoinSettings(false, GetStatic("stats/tpcds1000s.json"), true);
        auto db = kikimr.GetQueryClient();
        auto result = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session = result.GetSession();

        CreateTables(session, "schema/tpcds.sql", true);

        RunBenchmarkQueries(
            session,
            "TPCDS",
            ArcadiaSourceRoot() + "/ydb/library/benchmarks/gen_queries/consts.yql",
            {"yql"},
            99,
            "/ydb/library/benchmarks/queries/tpcds/",
            "/Root/test/ds/"
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH1) {
        CanonizedJoinOrderTest(
            "queries/tpch1.sql", "stats/tpch1000s.json", "join_order/tpch1_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH2) {
        CanonizedJoinOrderTest(
            "queries/tpch2.sql", "stats/tpch1000s.json", "join_order/tpch2_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH3) {
        CanonizedJoinOrderTest(
            "queries/tpch3.sql", "stats/tpch1000s.json", "join_order/tpch3_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH4) {
        CanonizedJoinOrderTest(
            "queries/tpch4.sql", "stats/tpch1000s.json", "join_order/tpch4_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH5) {
        CanonizedJoinOrderTest(
            "queries/tpch5.sql", "stats/tpch1000s.json", "join_order/tpch5_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH6) {
        CanonizedJoinOrderTest(
            "queries/tpch6.sql", "stats/tpch1000s.json", "join_order/tpch6_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH7) {
        CanonizedJoinOrderTest(
            "queries/tpch7.sql", "stats/tpch1000s.json", "join_order/tpch7_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH8) {
        CanonizedJoinOrderTest(
            "queries/tpch8.sql", "stats/tpch1000s.json", "join_order/tpch8_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH9) {
        CanonizedJoinOrderTest(
            "queries/tpch9.sql", "stats/tpch1000s.json", "join_order/tpch9_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH10) {
        CanonizedJoinOrderTest(
            "queries/tpch10.sql", "stats/tpch1000s.json", "join_order/tpch10_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH11) {
        CanonizedJoinOrderTest(
            "queries/tpch11.sql", "stats/tpch1000s.json", "join_order/tpch11_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH12) {
        CanonizedJoinOrderTest(
            "queries/tpch12.sql", "stats/tpch1000s.json", "join_order/tpch12_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH13) {
        CanonizedJoinOrderTest(
            "queries/tpch13.sql", "stats/tpch1000s.json", "join_order/tpch13_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH14) {
        CanonizedJoinOrderTest(
            "queries/tpch14.sql", "stats/tpch1000s.json", "join_order/tpch14_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH15) {
        CanonizedJoinOrderTest(
            "queries/tpch15.sql", "stats/tpch1000s.json", "join_order/tpch15_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH16) {
        CanonizedJoinOrderTest(
            "queries/tpch16.sql", "stats/tpch1000s.json", "join_order/tpch16_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH17) {
        CanonizedJoinOrderTest(
            "queries/tpch17.sql", "stats/tpch1000s.json", "join_order/tpch17_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH18) {
        CanonizedJoinOrderTest(
            "queries/tpch18.sql", "stats/tpch1000s.json", "join_order/tpch18_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH19) {
        CanonizedJoinOrderTest(
            "queries/tpch19.sql", "stats/tpch1000s.json", "join_order/tpch19_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH20) {
        CanonizedJoinOrderTest(
            "queries/tpch20.sql", "stats/tpch1000s.json", "join_order/tpch20_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH21) {
        CanonizedJoinOrderTest(
            "queries/tpch21.sql", "stats/tpch1000s.json", "join_order/tpch21_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCH22) {
        CanonizedJoinOrderTest(
            "queries/tpch22.sql", "stats/tpch1000s.json", "join_order/tpch22_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCDS64) {
        CanonizedJoinOrderTest(
            "queries/tpcds64.sql", "stats/tpcds1000s.json", "join_order/tpcds64_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCDS64_small) {
        CanonizedJoinOrderTest(
            "queries/tpcds64_small.sql", "stats/tpcds1000s.json", "join_order/tpcds64_small_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCDS78) {
        CanonizedJoinOrderTest(
            "queries/tpcds78.sql", "stats/tpcds1000s.json", "join_order/tpcds78_1000s.json", false, true
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderTPCC) {
        CanonizedJoinOrderTest(
            "queries/tpcc.sql", "stats/tpcc.json", "join_order/tpcc.json", false, false
        );
    }

    Y_UNIT_TEST(CanonizedJoinOrderLookupBug) {
        CanonizedJoinOrderTest(
            "queries/lookupbug.sql", "stats/lookupbug.json", "join_order/lookupbug.json", false, false
        );
    }

}
}
}
