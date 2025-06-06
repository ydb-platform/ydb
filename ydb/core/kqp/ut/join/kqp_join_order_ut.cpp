#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
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

void CreateTables(TSession session, const TString& schemaPath, bool useColumnStore) {
    std::string query = GetStatic(schemaPath);

    if (useColumnStore) {
        std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
        query = std::regex_replace(query, pattern, "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    }

    auto res = session.ExecuteSchemeQuery(TString(query)).GetValueSync();
    res.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(res.IsSuccess());
}

void CreateTablesGeneric(NYdb::NQuery::TSession session, const TString& schemaPath, bool useColumnStore) {
    std::string query = GetStatic(schemaPath);

    if (useColumnStore) {
        std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
        query = std::regex_replace(query, pattern, "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    }

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
static void CreateSampleTable(TSession session, bool useColumnStore) {
    CreateTables(session, "schema/rstuv.sql", useColumnStore);

    CreateTables(session, "schema/tpch.sql", useColumnStore);

    CreateTables(session, "schema/tpcds.sql", useColumnStore);

    CreateTables(session, "schema/tpcc.sql", useColumnStore);

    CreateTables(session, "schema/lookupbug.sql", useColumnStore);
}

static void CreateSampleTableGeneric(NYdb::NQuery::TSession session, bool useColumnStore) {
    CreateTablesGeneric(session, "schema/rstuv.sql", useColumnStore);

    CreateTablesGeneric(session, "schema/tpch.sql", useColumnStore);

    CreateTablesGeneric(session, "schema/tpcds.sql", useColumnStore);

    CreateTablesGeneric(session, "schema/tpcc.sql", useColumnStore);

    CreateTablesGeneric(session, "schema/lookupbug.sql", useColumnStore);

    CreateTablesGeneric(session, "schema/gpb.sql", false);
}

static TKikimrRunner GetKikimrWithJoinSettings(bool useStreamLookupJoin = false, TString stats = "", bool useCBO = true){
    Y_UNUSED(useCBO);

    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;

    setting.SetName("EnableKqpDataQueryStreamLookup");
    setting.SetValue("true");
    settings.push_back(setting); 

    if (stats != "") {
        setting.SetName("OptOverrideStatistics");
        setting.SetValue(stats);
        settings.push_back(setting);
    }

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(useStreamLookupJoin);
    appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
    appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());

    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    serverSettings.SetKqpSettings(settings);
    return TKikimrRunner(serverSettings);
}

void PrintPlan(const TString& plan) {
    // Cout << plan << Endl;
    // NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, true, Cout, 0);
    // queryPlanPrinter.Print(plan);

    std::string joinOrder = GetJoinOrder(plan).GetStringRobust();

    Cout << "JoinOrder: " << joinOrder << Endl;
    // std::replace(joinOrder.begin(), joinOrder.end(), '[', '(');
    // std::replace(joinOrder.begin(), joinOrder.end(), ']', ')');
    // std::replace(joinOrder.begin(), joinOrder.end(), ',', ' ');
    // joinOrder.erase(std::remove(joinOrder.begin(), joinOrder.end(), '\"'), joinOrder.end());
    // Cout << "JoinOrder" << joinOrder << Endl;
}

class TChainTester {
public:
    TChainTester(size_t chainSize)
        : Kikimr(GetKikimrWithJoinSettings(false, GetStats(chainSize)))
        , TableClient(Kikimr.GetTableClient())
        , Session(TableClient.CreateSession().GetValueSync().GetSession())
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
        PrintPlan(result.GetPlan());
    }

    TKikimrRunner Kikimr;
    NYdb::NTable::TTableClient TableClient;
    TSession Session;
    size_t ChainSize; 
};

void ExplainJoinOrderTestDataQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session, useColumnStore);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);
        
        auto result = session.ExplainDataQuery(query).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        PrintPlan(result.GetPlan());
    }
}

void TestOlapEstimationRowsCorrectness(const TString& queryPath, const TString& statsPath) {
    auto kikimr = GetKikimrWithJoinSettings(false, GetStatic(statsPath));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session, true);

    const TString actualQuery = GetStatic(queryPath);
    TString actualPlan;
    {
        auto result = session.ExplainDataQuery(actualQuery).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        actualPlan = result.GetPlan();
        PrintPlan(actualPlan);
        Cout << result.GetAst() << Endl;
    }

    const TString expectedQuery = R"(PRAGMA kikimr.OptEnableOlapPushdown = "false";)" "\n" + actualQuery;
    TString expectedPlan;
    {
        auto result = session.ExplainDataQuery(expectedQuery).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        expectedPlan = result.GetPlan();
        PrintPlan(expectedPlan);
        Cout << result.GetAst() << Endl;
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

    Y_UNIT_TEST(TPCDS87) {
        TestOlapEstimationRowsCorrectness("queries/tpcds87.sql", "stats/tpcds1000s.json");
    }

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
    //Y_UNIT_TEST(Chain65Nodes) {
    //    TChainTester(65).Test();
    //}

    TString ExecuteJoinOrderTestGenericQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore, bool useCBO = true, TMaybe<TString> dataQuery = {}, TMaybe<TString> canonicalResult = {}) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), useCBO);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSampleTableGeneric(session, useColumnStore);
        sleep(5);

        TString plan;
        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .ExecMode(NYdb::NQuery::EExecMode::Explain);
            
            auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            PrintPlan(*result.GetStats()->GetPlan());
            Cout << *result.GetStats()->GetAst() << Endl;
            plan = *result.GetStats()->GetPlan();
        }

        if (dataQuery) {
            Cerr << "actually run queries" << Endl;
            TString query = GetStatic(*dataQuery);
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .ExecMode(NYdb::NQuery::EExecMode::Execute);
            
            auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            query = GetStatic(queryPath);
            result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            if (canonicalResult) {
                Cerr << "checking results" << Endl;
               CompareYson(*canonicalResult, FormatResultSetYson(result.GetResultSet(0)));
            }
        }

        return plan;
    }

    TString ExecuteJoinOrderTestDataQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore, bool useCBO = true) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath), useCBO);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
            
            auto execRes = db.StreamExecuteScanQuery(query, TStreamExecScanQuerySettings().Explain(true)).ExtractValueSync();
            execRes.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(execRes.GetStatus(), EStatus::SUCCESS);
            auto plan = CollectStreamResult(execRes).PlanJson;
            PrintPlan(plan.GetRef());
            return plan.GetRef();
        }
    }

    void CheckJoinCardinality(const TString& queryPath, const TString& statsPath, const TString& joinKind, double card, bool useStreamLookupJoin, bool useColumnStore) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);

            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            PrintPlan(result.GetPlan());
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);

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
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/five_way_join.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinStatsOverride, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/five_way_join_stats_override.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FourWayJoinLeftFirst, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/four_way_join_left_first.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithPreds, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/five_way_join_with_preds.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithComplexPreds, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/five_way_join_with_complex_preds.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithComplexPreds2, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/five_way_join_with_complex_preds2.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithPredsAndEquiv, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats(
            "queries/four_way_join_with_preds_and_equiv.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FourWayJoinWithPredsAndEquivAndLeft, StreamLookupJoin, ColumnStore) {
       ExecuteJoinOrderTestDataQueryWithStats(
        "queries/four_way_join_with_preds_and_equiv_and_left.sql", "stats/basic.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithConstantFold, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_constant_fold.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(FiveWayJoinWithConstantFoldOpt, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_constant_fold_opt.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(DatetimeConstantFold, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/datetime_constant_fold.sql", "stats/basic.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH3, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch3.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH5, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch5.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH8, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch8.sql", "stats/tpch100s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH10, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch10.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH11, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch11.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCH21, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch21.sql", "stats/tpch1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS16, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds16.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);       
    }

    /* tpcds23 has > 1 result sets */
    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS23, StreamLookupJoin, ColumnStore) {
        ExplainJoinOrderTestDataQueryWithStats(
            "queries/tpcds23.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST(GPB) {
        SetDefaultIteratorQuotaSettings(1, 1e7);
        ExecuteJoinOrderTestGenericQueryWithStats("queries/gpb.sql", "stats/gpb.json", true, false, true,
            "queries/gpb-data.sql", R"([[99];[100];[103]])");
    }

    Y_UNIT_TEST(GPB2) {
        ExecuteJoinOrderTestGenericQueryWithStats("queries/gpb2.sql", "stats/gpb.json", true, false);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS34, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds34.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);       
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS61, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds61.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);       
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS87, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds87.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS88, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds88.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore); 
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS90, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds90.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);  
    }
    
    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS92, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds92.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS94, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds94.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore); 
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS95, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds95.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore); 
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TPCDS96, StreamLookupJoin, ColumnStore) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds96.sql", "stats/tpcds1000s.json", StreamLookupJoin, ColumnStore);     
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint1, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint1.sql", "stats/basic.json", "InnerJoin (Grace)", 10e6, StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint2, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint2.sql", "stats/basic.json", "InnerJoin (MapJoin)", 1, StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsSimple, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestDataQueryWithStats("queries/join_order_hints_simple.sql", "stats/basic.json", StreamLookupJoin, ColumnStore); 
        UNIT_ASSERT_VALUES_EQUAL(GetJoinOrder(plan).GetStringRobust(), R"(["T",["R","S"]])") ;
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsComplex, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestDataQueryWithStats("queries/join_order_hints_complex.sql", "stats/basic.json", StreamLookupJoin, ColumnStore); 
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"([["R","S"],["T","U"]])") != TString::npos, joinOrder);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinOrderHintsManyHintTrees, StreamLookupJoin, ColumnStore) {
        auto plan = ExecuteJoinOrderTestDataQueryWithStats("queries/join_order_hints_many_hint_trees.sql", "stats/basic.json", StreamLookupJoin, ColumnStore); 
        auto joinOrder = GetJoinOrder(plan).GetStringRobust();
        UNIT_ASSERT_C(joinOrder.find(R"(["R","S"])") != TString::npos, joinOrder);
        UNIT_ASSERT_C(joinOrder.find(R"(["T","U"])") != TString::npos, joinOrder);
    }



    void CanonizedJoinOrderTest(const TString& queryPath, const TString& statsPath, TString correctJoinOrderPath, bool useStreamLookupJoin, bool useColumnStore
    ) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
        
            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            PrintPlan(result.GetPlan());
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            if (useStreamLookupJoin) {
                return;
            }

            if (useColumnStore) {
                correctJoinOrderPath = correctJoinOrderPath.substr(0, correctJoinOrderPath.find(".json")) + "_column_store.json";      
            }

            auto currentJoinOrder = GetPrettyJSON(GetDetailedJoinOrder(result.GetPlan()));

            /* to canonize the tests use --test-param CANONIZE_JOIN_ORDER_TESTS=TRUE */
            TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS"); canonize.to_lower();
            if (canonize.equal("true")) {
                Cerr << "--------------------CANONIZING THE TESTS--------------------";
                TOFStream stream(SRC_("data/" + correctJoinOrderPath));
                stream << currentJoinOrder << Endl;
            }

            TString ref = GetStatic(correctJoinOrderPath);
            Cout << "actual\n" << GetJoinOrder(result.GetPlan()).GetStringRobust() << Endl; 
            Cout << "expected\n" << GetJoinOrderFromDetailedJoinOrder(ref).GetStringRobust() << Endl;
            UNIT_ASSERT(JoinOrderAndAlgosMatch(result.GetPlan(), ref));
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
