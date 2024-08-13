#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

#include <fstream>

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

/*
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
 */
static void CreateSampleTable(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/rstuv.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/R` (id, payload1, ts) VALUES (1, "blah", CAST("1998-12-01" AS Date) );
        REPLACE INTO `/Root/S` (id, payload2) VALUES (1, "blah");
        REPLACE INTO `/Root/T` (id, payload3) VALUES (1, "blah");
        REPLACE INTO `/Root/U` (id, payload4) VALUES (1, "blah");
        REPLACE INTO `/Root/V` (id, payload5) VALUES (1, "blah");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpch.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpcds.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpcc.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/lookupbug.sql")).GetValueSync().IsSuccess());

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

class TChainConstructor {
public:
    TChainConstructor(size_t chainSize)
        : Kikimr_(GetKikimrWithJoinSettings())
        , TableClient_(Kikimr_.GetTableClient())
        , Session_(TableClient_.CreateSession().GetValueSync().GetSession())
        , ChainSize_(chainSize)
    {}

    void CreateTables() {
        for (size_t i = 0; i < ChainSize_; ++i) {
            TString tableName;
            
            tableName
                .append("/Root/table_").append(ToString(i));;

            TString createTable;
            createTable
                += "CREATE TABLE `" +  tableName + "` (id"
                +  ToString(i) + " Int32, " 
                +  "PRIMARY KEY (id" + ToString(i) + "));";

            std::cout << createTable << std::endl;
            auto res = Session_.ExecuteSchemeQuery(createTable).GetValueSync();
            std::cout << res.GetIssues().ToString() << std::endl;
            UNIT_ASSERT(res.IsSuccess());
        }
    }

    void JoinTables() {
        TString joinRequest;

        joinRequest.append("SELECT * FROM `/Root/table_0` as t0 ");

        for (size_t i = 1; i < ChainSize_; ++i) {
            TString table = "/Root/table_" + ToString(i);

            TString prevAliasTable = "t" + ToString(i - 1);
            TString aliasTable = "t" + ToString(i);

            joinRequest
                += "INNER JOIN `" + table + "`" + " AS " + aliasTable + " ON "
                +  aliasTable + ".id" + ToString(i) + "=" + prevAliasTable + ".id" 
                +  ToString(i-1) + " ";
        }

        auto result = Session_.ExecuteDataQuery(joinRequest, TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        std::cout << result.GetIssues().ToString() << std::endl;
        std::cout << joinRequest << std::endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

private:
    TKikimrRunner Kikimr_;
    NYdb::NTable::TTableClient TableClient_;
    TSession Session_;
    size_t ChainSize_; 
};

void ExplainJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);
        
        auto result = session.ExplainDataQuery(query).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cout << result.GetPlan() << Endl;
        Cout << CanonizeJoinOrder(result.GetPlan()) << Endl;
    }
}

void ExecuteJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);

        auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(Chain65Nodes) {
        TChainConstructor chain(65);
        chain.CreateTables();
        chain.JoinTables();
    }

    void ExecuteJoinOrderTestDataQueryWithStats(const TString& queryPath, const TString& statsPath, bool useStreamLookupJoin) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);

            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            Cout << result.GetPlan() << Endl;
        
            auto result2 = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result2.GetStatus(), EStatus::SUCCESS);
        }
    }

    void CheckJoinCardinality(const TString& queryPath, const TString& statsPath, const TString& joinKind, double card, bool useStreamLookupJoin) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);

            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            Cout << result.GetPlan() << Endl;
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

    Y_UNIT_TEST_TWIN(FiveWayJoin, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinStatsOverride, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_stats_override.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FourWayJoinLeftFirst, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/four_way_join_left_first.sql", "stats/basic.json", StreamLookupJoin);
    }

     Y_UNIT_TEST_TWIN(FiveWayJoinWithPreds, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_preds.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_complex_preds.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds2, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_complex_preds2.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithPredsAndEquiv, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/four_way_join_with_preds_and_equiv.sql", "stats/basic.json", StreamLookupJoin);
    }

    //Y_UNIT_TEST_TWIN(FourWayJoinWithPredsAndEquivAndLeft, StreamLookupJoin) {
    //    ExecuteJoinOrderTestDataQueryWithStats("queries/four_way_join_with_preds_and_equiv_and_left.sql", "stats/basic.json", StreamLookupJoin);
    //}

    Y_UNIT_TEST_TWIN(TestJoinHint, StreamLookupJoin) {
        CheckJoinCardinality("queries/test_join_hint.sql", "stats/basic.json", "InnerJoin (Grace)", 10e6, StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TestJoinHint2, StreamLookupJoin) {
        CheckJoinCardinality("queries/test_join_hint2.sql", "stats/basic.json", "InnerJoin (MapJoin)", 1, StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFold, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_constant_fold.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFoldOpt, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/five_way_join_with_constant_fold_opt.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(DatetimeConstantFold, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/datetime_constant_fold.sql", "stats/basic.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH3, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch3.sql", "stats/tpch1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH5, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch5.sql", "stats/tpch1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH10, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch10.sql", "stats/tpch1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH11, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch11.sql", "stats/tpch1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH21, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpch21.sql", "stats/tpch1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCDS16, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds16.sql", "stats/tpcds1000s.json", StreamLookupJoin);       
    }

    Y_UNIT_TEST_TWIN(TPCDS61, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds61.sql", "stats/tpcds1000s.json", StreamLookupJoin);       
    }

    Y_UNIT_TEST_TWIN(TPCDS87, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds87.sql", "stats/tpcds1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCDS88, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds88.sql", "stats/tpcds1000s.json", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS90, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds90.sql", "stats/tpcds1000s.json", StreamLookupJoin);  
    }
    
    Y_UNIT_TEST_TWIN(TPCDS92, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds92.sql", "stats/tpcds1000s.json", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCDS94, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds94.sql", "stats/tpcds1000s.json", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS95, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds95.sql", "stats/tpcds1000s.json", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS96, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQueryWithStats("queries/tpcds96.sql", "stats/tpcds1000s.json", StreamLookupJoin);     
    }

    void JoinOrderTestWithOverridenStats(const TString& queryPath, const TString& statsPath, const TString& correctJoinOrderPath, bool useStreamLookupJoin) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
        
            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString ref = GetStatic(correctJoinOrderPath);

            Cout << result.GetPlan() << Endl;

            /* correct canonized join order in cout, change corresponding join_order/.json file */
            Cout << CanonizeJoinOrder(result.GetPlan()) << Endl;

            /* Only check the plans if stream join is enabled*/
            if (useStreamLookupJoin) {
                UNIT_ASSERT(JoinOrderAndAlgosMatch(result.GetPlan(), ref));
            }
        }
    }

    Y_UNIT_TEST_TWIN(OverrideStatsTPCH2, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch2.sql", "stats/tpch1000s.json", "join_order/tpch2_1000s.json", StreamLookupJoin
        );
    }

    Y_UNIT_TEST_TWIN(OverrideStatsTPCH9, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch9.sql", "stats/tpch1000s.json", "join_order/tpch9_1000s.json", StreamLookupJoin
        );
    }


    Y_UNIT_TEST_TWIN(OverrideStatsTPCDS64, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds64.sql", "stats/tpcds1000s.json", "join_order/tpcds64_1000s.json", StreamLookupJoin
        );
    }


    Y_UNIT_TEST_TWIN(OverrideStatsTPCDS64_small, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds64_small.sql", "stats/tpcds1000s.json", "join_order/tpcds64_small_1000s.json", StreamLookupJoin
        );
    }
   
    Y_UNIT_TEST_TWIN(OverrideStatsTPCDS78, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds78.sql", "stats/tpcds1000s.json", "join_order/tpcds78_1000s.json", StreamLookupJoin
        );
    }

    Y_UNIT_TEST(TPCC) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcc.sql", "stats/tpcc.json", "join_order/tpcc.json", false);
    }

    Y_UNIT_TEST(LookupBug) {
        JoinOrderTestWithOverridenStats(
            "queries/lookupbug.sql", "stats/lookupbug.json", "join_order/lookupbug.json", false);
    }


}
}
}
