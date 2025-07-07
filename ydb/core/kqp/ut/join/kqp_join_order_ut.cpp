#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/string/printf.h>

#include <algorithm>
#include <fstream>
#include <regex>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

#define Y_UNIT_TEST_XOR_OR_BOTH_FALSE(N, OPT1, OPT2)                                                                                              \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N;                                                                                 \
    template<bool OPT1, bool OPT2>                                                                                                   \
    void N(NUnitTest::TTestContext&)

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

    CreateTables(session, "schema/general_priorities_bug.sql", useColumnStore);

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
    appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
    appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());
    appConfig.MutableFeatureFlags()->SetEnableViews(true);
    if (!useCBO) {
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(0);
    } else {
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
    }
    appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(true);

    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    serverSettings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(params.EnableSeparationComputeActorsFromRead);
    serverSettings.SetKqpSettings(settings);
    return TKikimrRunner(serverSettings);
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
    Cout << "JoinOrder" << joinOrder << Endl;
}

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
    kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

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
    kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

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

    TString ExecuteJoinOrderTestGenericQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore, bool useCBO = true) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), useCBO);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);

            auto execRes = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            execRes.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(execRes.GetStatus(), EStatus::SUCCESS);

            auto explainRes = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            explainRes.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(explainRes.GetStatus(), EStatus::SUCCESS);

            PrintPlan(*explainRes.GetStats()->GetPlan());
            return *explainRes.GetStats()->GetPlan();
        }
    }

    void CheckJoinCardinality(const TString& queryPath, const TString& statsPath, const TString& joinKind, double card, bool useStreamLookupJoin, bool useColumnStore) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

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

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoin, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinStatsOverride, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_stats_override.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FourWayJoinLeftFirst, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/four_way_join_left_first.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithPreds, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_preds.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithComplexPreds, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_complex_preds.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithComplexPreds2, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/five_way_join_with_complex_preds2.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithPredsAndEquiv, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/four_way_join_with_preds_and_equiv.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FourWayJoinWithPredsAndEquivAndLeft, StreamLookupJoin, ColumnStore) {
       ExecuteJoinOrderTestGenericQueryWithStats(
        "queries/four_way_join_with_preds_and_equiv_and_left.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithConstantFold, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/five_way_join_with_constant_fold.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithConstantFoldOpt, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/five_way_join_with_constant_fold_opt.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(DatetimeConstantFold, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/datetime_constant_fold.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(UdfConstantFold, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/udf_constant_fold.sql", "stats/basic.json", false, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCHRandomJoinViewJustWorks, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch_random_join_view_just_works.sql", "stats/tpch1000s.json", false, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH3, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch3.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH5, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch5.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH8, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch8.sql", "stats/tpch100s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH10, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch10.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH11, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch11.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH21, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpch21.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS16, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds16.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    /* tpcds23 has > 1 result sets */
    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS23, StreamLookupJoin, ColumnStore) {
        ExplainJoinOrderTestDataQueryWithStats(
            "queries/tpcds23.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    bool CheckLimitOnlyNotTopSort(const TString& plan) {
        return plan.Contains("Limit") && !plan.Contains("Top");
    }

    Y_UNIT_TEST(GeneralPrioritiesBug1) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/general_priorities_bug.sql", "stats/general_priorities_bug.json", true, false);
        UNIT_ASSERT(CheckLimitOnlyNotTopSort(plan));
    }

    Y_UNIT_TEST(GeneralPrioritiesBug2) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/general_priorities_bug2.sql", "stats/general_priorities_bug.json", true, false);
        UNIT_ASSERT(CheckLimitOnlyNotTopSort(plan));
    }

    Y_UNIT_TEST(GeneralPrioritiesBug3) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/general_priorities_bug3.sql", "stats/general_priorities_bug.json", true, false);
        UNIT_ASSERT(CheckLimitOnlyNotTopSort(plan));
    }

    Y_UNIT_TEST(GeneralPrioritiesBug4) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/general_priorities_bug4.sql", "stats/general_priorities_bug.json", true, false);
        UNIT_ASSERT(CheckLimitOnlyNotTopSort(plan));
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS34, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds34.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS61, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds61.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS87, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds87.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS88, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds88.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS90, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds90.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS92, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds92.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS94, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds94.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS95, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds95.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS96, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/tpcds96.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint1, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint1.sql", "stats/basic.json", "InnerJoin (Grace)", 10e6, StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint2, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint2.sql", "stats/basic.json", "InnerJoin (MapJoin)", 1, StreamLookupJoin, ColumnStore);
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

    Y_UNIT_TEST(ShuffleEliminationOneJoin) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/shuffle_elimination_one_join.sql", "stats/tpch1000s.json", false, true, true);
        auto joinFinder = TFindJoinWithLabels(plan);
        auto join = joinFinder.Find({"customer", "orders"});
        UNIT_ASSERT_C(join.Join == "InnerJoin (Grace)", join.Join);
        UNIT_ASSERT(!join.LhsShuffled);
        UNIT_ASSERT(join.RhsShuffled);
    }

    Y_UNIT_TEST(ShuffleEliminationReuseShuffleTwoJoins) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/shuffle_elimination_reuse_shuffle_two_joins.sql", "stats/tpch1000s.json", false, true, true);
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

    Y_UNIT_TEST(ShuffleEliminationManyKeysJoinPredicate) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_many_keys_join_predicate.sql", "stats/tpch1000s.json", false, true, true
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        {
            auto join = joinFinder.Find({"partsupp", "lineitem"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (Grace)");
            UNIT_ASSERT(!join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }
    }

    Y_UNIT_TEST(ShuffleEliminationTpcdsMapJoinBug) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats(
            "queries/shuffle_elimination_tpcds_map_join_bug.sql", "stats/tpcds1000s.json", false, true, true
        );

        auto joinFinder = TFindJoinWithLabels(plan);
        {
            auto join = joinFinder.Find({"test/ds/customer", "test/ds/customer_address"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (MapJoin)");
        }
        {
            auto join = joinFinder.Find({"test/ds/customer_demographics", "test/ds/customer", "test/ds/customer_address"});
            UNIT_ASSERT_EQUAL(join.Join, "InnerJoin (MapJoin)");
        }
        {
            auto join = joinFinder.Find({"test/ds/customer_demographics", "test/ds/customer", "test/ds/customer_address", "test/ds/store_sales"});
            UNIT_ASSERT_EQUAL(join.Join, "LeftSemiJoin (Grace)");
            UNIT_ASSERT(join.LhsShuffled);
            UNIT_ASSERT(join.RhsShuffled);
        }
    }


    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsSimple, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_simple.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
        UNIT_ASSERT_VALUES_EQUAL(GetJoinOrder(plan).GetStringRobust(), R"(["T",["R","S"]])") ;
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsComplex, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_complex.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"([["R","S"],["T","U"]])") != TString::npos, joinOrder);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsManyHintTrees, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestGenericQueryWithStats("queries/join_order_hints_many_hint_trees.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"(["R","S"])") != TString::npos, joinOrder);
        UNIT_ASSERT_C(joinOrder.find(R"(["T","U"])") != TString::npos, joinOrder);
    }



    void CanonizedJoinOrderTest(const TString& queryPath, const TString& statsPath, TString correctJoinOrderPath, bool useStreamLookupJoin, bool useColumnStore
    ) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

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
            PrintPlan(TString{*result.GetStats()->GetPlan()});
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            if (useStreamLookupJoin) {
                return;
            }

            if (useColumnStore) {
                correctJoinOrderPath = correctJoinOrderPath.substr(0, correctJoinOrderPath.find(".json")) + "_column_store.json";
            }

            auto currentJoinOrder = GetPrettyJSON(GetDetailedJoinOrder(TString{*result.GetStats()->GetPlan()}));

            /* to canonize the tests use --test-param CANONIZE_JOIN_ORDER_TESTS=TRUE */
            TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS"); canonize.to_lower();
            if (canonize.equal("true")) {
                Cerr << "--------------------CANONIZING THE TESTS--------------------";
                TOFStream stream(SRC_("data/" + correctJoinOrderPath));
                stream << currentJoinOrder << Endl;
            }

            TString ref = GetStatic(correctJoinOrderPath);
            Cout << "actual\n" << GetJoinOrder(TString{*result.GetStats()->GetPlan()}).GetStringRobust() << Endl;
            Cout << "expected\n" << GetJoinOrderFromDetailedJoinOrder(ref).GetStringRobust() << Endl;
            UNIT_ASSERT(JoinOrderAndAlgosMatch(TString{*result.GetStats()->GetPlan()}, ref));
        }
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(CanonizedJoinOrderTPCH2, StreamLookupJoin, ColumnStore) {
        CanonizedJoinOrderTest(
            "queries/tpch2.sql", "stats/tpch1000s.json", "join_order/tpch2_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(CanonizedJoinOrderTPCH9, StreamLookupJoin, ColumnStore) {
        CanonizedJoinOrderTest(
            "queries/tpch9.sql", "stats/tpch1000s.json", "join_order/tpch9_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(CanonizedJoinOrderTPCDS64, StreamLookupJoin, ColumnStore) {
        CanonizedJoinOrderTest(
            "queries/tpcds64.sql", "stats/tpcds1000s.json", "join_order/tpcds64_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(CanonizedJoinOrderTPCDS64_small, StreamLookupJoin, ColumnStore) {
        CanonizedJoinOrderTest(
            "queries/tpcds64_small.sql", "stats/tpcds1000s.json", "join_order/tpcds64_small_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(CanonizedJoinOrderTPCDS78, StreamLookupJoin, ColumnStore) {
        CanonizedJoinOrderTest(
            "queries/tpcds78.sql", "stats/tpcds1000s.json", "join_order/tpcds78_1000s.json", StreamLookupJoin, ColumnStore
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