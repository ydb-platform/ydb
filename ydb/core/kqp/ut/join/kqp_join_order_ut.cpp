#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

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

static TKikimrRunner GetKikimrWithJoinSettings(bool useStreamLookupJoin = false, TString stats = ""){
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

    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    serverSettings.SetKqpSettings(settings);
    return TKikimrRunner(serverSettings);
}

class TChainTester {
public:
    TChainTester(size_t chainSize)
        : Kikimr(GetKikimrWithJoinSettings())
        , TableClient(Kikimr.GetTableClient())
        , Session(TableClient.CreateSession().GetValueSync().GetSession())
        , ChainSize(chainSize)
    {}

public:
    void Test() {
        CreateTables();
        JoinTables();
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

        auto result = Session.ExecuteDataQuery(joinRequest, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    TKikimrRunner Kikimr;
    NYdb::NTable::TTableClient TableClient;
    TSession Session;
    size_t ChainSize; 
};

void ExplainJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin, bool useColumnStore) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session, useColumnStore);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);
        
        auto result = session.ExplainDataQuery(query).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cerr << result.GetPlan() << Endl;
        Cerr << CanonizeJoinOrder(result.GetPlan()) << Endl;
    }
}

void ExecuteJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin, bool useColumnStore) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session, useColumnStore);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);
        
        auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(Chain65Nodes) {
        TChainTester(65).Test();
    }

    void ExecuteJoinOrderTestDataQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin, bool useColumnStore) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session, useColumnStore);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
            
            auto execRes = db.StreamExecuteScanQuery(query, TStreamExecScanQuerySettings().Explain(true)).ExtractValueSync();
            execRes.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(execRes.GetStatus(), EStatus::SUCCESS);
            Cerr << CollectStreamResult(execRes).PlanJson;
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
            Cerr << result.GetPlan() << Endl;
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

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint.sql", "stats/basic.json", "InnerJoin (Grace)", 10e6, StreamLookupJoin, ColumnStore);
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(TestJoinHint2, StreamLookupJoin, ColumnStore) {
        CheckJoinCardinality("queries/test_join_hint2.sql", "stats/basic.json", "InnerJoin (MapJoin)", 1, StreamLookupJoin, ColumnStore);
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

    void JoinOrderTestWithOverridenStats(const TString& queryPath, const TString& statsPath, TString correctJoinOrderPath, bool useStreamLookupJoin, bool useColumnStore
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
            Cerr << result.GetPlan() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            if (useStreamLookupJoin) {
                return;
            }

            if (useColumnStore) {
                correctJoinOrderPath = correctJoinOrderPath.substr(0, correctJoinOrderPath.find(".json")) + "_column_store.json";      
            }

            auto currentJoinOrder = CanonizeJoinOrder(result.GetPlan());
            Cerr << currentJoinOrder << Endl;
            /* to canonize the tests use --test-param CANONIZE_JOIN_ORDER_TESTS=TRUE */
            TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS"); canonize.to_lower();
            if (canonize.equal("true")) {
                Cerr << "--------------------CANONIZING THE TESTS--------------------";
                TOFStream stream(SRC_("data/" + correctJoinOrderPath));
                NJsonWriter::TBuf writer;
                writer.SetIndentSpaces(2);
                writer.WriteJsonValue(&currentJoinOrder);
                writer.FlushTo(&stream);
                stream << Endl;
            }

            TString ref = GetStatic(correctJoinOrderPath);
            UNIT_ASSERT(JoinOrderAndAlgosMatch(result.GetPlan(), ref));
        }
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(OverrideStatsTPCH2, StreamLookupJoin, ColumnStore) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch2.sql", "stats/tpch1000s.json", "join_order/tpch2_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(OverrideStatsTPCH9, StreamLookupJoin, ColumnStore) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch9.sql", "stats/tpch1000s.json", "join_order/tpch9_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(OverrideStatsTPCDS64, StreamLookupJoin, ColumnStore) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds64.sql", "stats/tpcds1000s.json", "join_order/tpcds64_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(OverrideStatsTPCDS64_small, StreamLookupJoin, ColumnStore) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds64_small.sql", "stats/tpcds1000s.json", "join_order/tpcds64_small_1000s.json", StreamLookupJoin, ColumnStore
        );
    }
   
    Y_UNIT_TEST_XOR_OR_BOTH_FALSE(OverrideStatsTPCDS78, StreamLookupJoin, ColumnStore) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds78.sql", "stats/tpcds1000s.json", "join_order/tpcds78_1000s.json", StreamLookupJoin, ColumnStore
        );
    }

    Y_UNIT_TEST(TPCC) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcc.sql", "stats/tpcc.json", "join_order/tpcc.json", false, false
        );
    }

    Y_UNIT_TEST(LookupBug) {
        JoinOrderTestWithOverridenStats(
            "queries/lookupbug.sql", "stats/lookupbug.json", "join_order/lookupbug.json", false, false
        );
    }

}
}
}
