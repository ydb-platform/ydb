#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
void CreateSampleTables(TKikimrRunner& kikimr) {
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "FourShard"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value1", Type: "String" }
        Columns { Name: "Value2", Type: "String" }
        KeyColumnNames: ["Key"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 300 } } } }
    )");

    TTableClient tableClient{kikimr.GetDriver()};
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/FourShard` (Key, Value1, Value2) VALUES
            (1u,   "Value-001",  "1"),
            (2u,   "Value-002",  "2"),
            (101u, "Value-101",  "101"),
            (102u, "Value-102",  "102"),
            (201u, "Value-201",  "201"),
            (202u, "Value-202",  "202"),
            (301u, "Value-301",  "301"),
            (302u, "Value-302",  "302")
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    session.Close();
}

}

Y_UNIT_TEST_SUITE(KqpExplain) {

    Y_UNIT_TEST_TWIN(Explain, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);

        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan");
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "KeyValue");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST_TWIN(ExplainStream, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan");
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "KeyValue");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST_TWIN(ExplainScanQueryWithParams, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            --SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
            PRAGMA Kikimr.UseNewEngine = "false";
            DECLARE $value as Utf8;
            SELECT $value as value;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(AggGroupLimit, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT min(Message), max(Message) FROM `/Root/Logs` WHERE Ts > 1 and Ts <= 4 or App="ydb" GROUP BY App;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto read = FindPlanNodeByKv(plan, "Node Type", "Aggregate-Filter-TableFullScan");
        UNIT_ASSERT(read.IsDefined());
        auto tables = read.GetMapSafe().at("Tables").GetArraySafe();
        UNIT_ASSERT(tables[0].GetStringSafe() == "Logs");
        auto shuffle = FindPlanNodeByKv(plan, "Node Type", "HashShuffle");
        UNIT_ASSERT(shuffle.IsDefined());
        auto& columns = shuffle.GetMapSafe().at("KeyColumns").GetArraySafe();
        UNIT_ASSERT(!columns.empty() && columns[0] == "App");
        auto aggregate = FindPlanNodeByKv(read, "Name", "Aggregate");
        UNIT_ASSERT(aggregate.IsDefined());
        UNIT_ASSERT(aggregate.GetMapSafe().at("GroupBy").GetStringSafe() == "item.App");
        UNIT_ASSERT(aggregate.GetMapSafe().at("Aggregation").GetStringSafe() ==
            "{_yql_agg_0: MIN(item.Message),_yql_agg_1: MAX(item.Message)}");
    }

    Y_UNIT_TEST_TWIN(ComplexJoin, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        CreateSampleTables(kikimr);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            $join = (
                SELECT l.Key as Key, l.Text as Text, l.Data as Data, r.Value1 as Value1, r.Value2 as Value2
                FROM `/Root/EightShard` AS l JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            );
            SELECT Key, COUNT(*) AS Cnt
            FROM $join
            WHERE Cast(Data As Int64) < (Key - 100) and Value1 != 'Value-101'
            GROUP BY Key
            UNION ALL
            (SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1)
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto join = FindPlanNodeByKv(
            plan,
            "Node Type",
            "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan"
        );
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "FourShard");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST_TWIN(PrecomputeRange, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 149 + 1 AND 266 ORDER BY Data LIMIT 4;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableRangesScan");
        UNIT_ASSERT(node.IsDefined());

        auto operators = node.GetMapSafe().at("Operators").GetArraySafe();
        UNIT_ASSERT(operators[1].GetMapSafe().at("Name") == "TableRangesScan");

        auto& readRanges = operators[1].GetMapSafe().at("ReadRanges").GetArraySafe();
        UNIT_ASSERT(readRanges[0] == "Key [150, 266]");
    }

    Y_UNIT_TEST_TWIN(CompoundKeyRange, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            PRAGMA Kikimr.OptEnablePredicateExtract = "false";
            SELECT * FROM `/Root/Logs` WHERE App = "new_app_1" AND Host < "xyz" AND Ts = (42+7) Limit 10;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto read = FindPlanNodeByKv(plan, "Node Type", "Limit-TablePointLookup");
        auto& operators = read.GetMapSafe().at("Operators").GetArraySafe();
        UNIT_ASSERT(operators.size() == 2);

        auto& lookup = operators[1].GetMapSafe();
        UNIT_ASSERT(lookup.at("Name") == "TablePointLookup");
        UNIT_ASSERT(lookup.at("ReadRange").GetArraySafe()[0] == "App (new_app_1)");
    }

    Y_UNIT_TEST_TWIN(SortStage, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 150 AND 266 ORDER BY Text;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto scanSort = FindPlanNodeByKv(plan, "Node Type", "Sort-TableRangeScan");
        UNIT_ASSERT(scanSort.IsDefined());
    }

    Y_UNIT_TEST_TWIN(LimitOffset, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/EightShard` ORDER BY Text LIMIT 10 OFFSET 15;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto limit = FindPlanNodeByKv(plan, "Limit", "10");
        UNIT_ASSERT(limit.IsDefined());
        auto offset = FindPlanNodeByKv(plan, "Offset", "15");
        UNIT_ASSERT(offset.IsDefined());
    }

    Y_UNIT_TEST_TWIN(SelfJoin3xSameLabels, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            $foo = (
                SELECT t1.Key AS Key
                FROM `/Root/KeyValue` AS t1
                JOIN `/Root/KeyValue` AS t2
                ON t1.Key = t2.Key
                GROUP BY t1.Key
            );
            SELECT t1.Key AS Key
            FROM $foo AS Foo
            JOIN `/Root/KeyValue` AS t1
            ON t1.Key = Foo.Key
            ORDER BY Key
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto join1 = FindPlanNodeByKv(plan, "Node Type", "Sort-InnerJoin (MapJoin)-Filter-Aggregate");
        UNIT_ASSERT(join1.IsDefined());
        auto join2 = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter");
        UNIT_ASSERT(join2.IsDefined());
    }

    Y_UNIT_TEST_TWIN(PureExpr, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT 1,2,3 UNION ALL SELECT 4,5,6;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto constExpr = FindPlanNodeByKv(plan, "Node Type", "ConstantExpr");
        UNIT_ASSERT(constExpr.IsDefined());
    }

    Y_UNIT_TEST_TWIN(MultiUsedStage, UseSessionActor) {
        NKikimrConfig::TAppConfig appCfg;
        auto* spilling = appCfg.MutableTableServiceConfig()->MutableSpillingServiceConfig()->MutableLocalFileConfig();
        spilling->SetEnable(true);
        spilling->SetRoot("./spilling/");
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor, {}, appCfg);

        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            select count(*) from `/Root/KeyValue` AS t1 join `/Root/KeyValue` AS t2 on t1.Key = t2.Key;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        bool containCte = false;
        auto& plans = plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans");
        for (auto& planNode : plans.GetArraySafe()) {
            auto& planMap = planNode.GetMapSafe();
            if (planMap.contains("Subplan Name") && planMap["Subplan Name"].GetStringSafe().Contains("CTE")) {
                containCte = true;
                break;
            }
        }

        UNIT_ASSERT(containCte);
    }

    Y_UNIT_TEST_TWIN(SqlIn, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        CreateSampleTables(kikimr);

        TStreamExecScanQuerySettings settings;
        settings.Explain(true);
        auto db = kikimr.GetTableClient();

        auto query = R"(
                PRAGMA Kikimr.OptEnablePredicateExtract = "false";
                SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 42)
                ORDER BY Key
            )";

        auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto unionNode = FindPlanNodeByKv(plan, "Node Type", "Sort-Union");
        UNIT_ASSERT_EQUAL(unionNode.GetMap().at("Plans").GetArraySafe().size(), 4);
    }

    Y_UNIT_TEST_TWIN(ExplainDataQueryOldEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 42) ORDER BY Key;
        )").ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        UNIT_ASSERT_EQUAL(plan.GetMapSafe().at("tables").GetArraySafe()[0].GetMapSafe().at("name").GetStringSafe(), "/Root/KeyValue");
    }

    Y_UNIT_TEST_TWIN(ExplainDataQueryNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            PRAGMA kikimr.UseNewEngine = "true";
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 42) ORDER BY Key;
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 4*2) ORDER BY Key;
            SELECT count(distinct Value) FROM `/Root/KeyValue` WHERE Key > 20 and Key <= 120;
            SELECT count(distinct Value) FROM `/Root/KeyValue`;
            SELECT count(distinct Value) FROM `/Root/KeyValue` WHERE Key >= 10;
        )").ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto node = FindPlanNodeByKv(plan, "Name", "TableRangeScan");
        UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");
        node = FindPlanNodeByKv(plan, "Name", "TableFullScan");
        UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");
        node = FindPlanNodeByKv(plan, "Name", "TablePointLookup");
        UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");
    }

    Y_UNIT_TEST_TWIN(FewEffects, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            PRAGMA kikimr.UseNewEngine = "true";
            UPDATE `/Root/EightShard` SET Data=Data+1;
            UPDATE `/Root/EightShard` SET Data=Data-1 WHERE Key In (100,200,300);
            DELETE FROM `/Root/EightShard` WHERE Key > 350;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);

        // Cerr << plan << Endl;

        auto upsertsCount = CountPlanNodesByKv(plan, "Node Type", "Upsert-ConstantExpr");
        UNIT_ASSERT_VALUES_EQUAL(upsertsCount, 2);

        auto deletesCount = CountPlanNodesByKv(plan, "Node Type", "Delete-ConstantExpr");
        UNIT_ASSERT_VALUES_EQUAL(deletesCount, 1);

        auto fullScansCount = CountPlanNodesByKv(plan, "Node Type", "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL(fullScansCount, 1);

        auto rangeScansCount = CountPlanNodesByKv(plan, "Node Type", "TableRangeScan");
        UNIT_ASSERT_VALUES_EQUAL(rangeScansCount, 1);

        auto lookupsCount = CountPlanNodesByKv(plan, "Node Type", "TablePointLookup-ConstantExpr");
        UNIT_ASSERT_VALUES_EQUAL(lookupsCount, 3);

        /* check tables section */
        const auto& tableInfo = plan.GetMapSafe().at("tables").GetArraySafe()[0].GetMapSafe();
        UNIT_ASSERT_VALUES_EQUAL(tableInfo.at("name"), "/Root/EightShard");

        THashMap<TString, int> counter;
        auto countOperationsByType = [&tableInfo, &counter](const auto& type) {
            for (const auto& op : tableInfo.at(type).GetArraySafe()) {
                ++counter[op.GetMapSafe().at("type").GetStringSafe()];
            }
        };

        countOperationsByType("reads");
        countOperationsByType("writes");

        UNIT_ASSERT_VALUES_EQUAL(counter["MultiUpsert"], upsertsCount);
        UNIT_ASSERT_VALUES_EQUAL(counter["MultiErase"], deletesCount);
        UNIT_ASSERT_VALUES_EQUAL(counter["FullScan"], fullScansCount);
        UNIT_ASSERT_VALUES_EQUAL(counter["Scan"], rangeScansCount);
        UNIT_ASSERT_VALUES_EQUAL(counter["Lookup"], lookupsCount);
    }

    Y_UNIT_TEST_TWIN(ExplainDataQueryWithParams, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            PRAGMA Kikimr.UseNewEngine = "false";
            DECLARE $value as Utf8;
            SELECT $value as value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto result2 = session.ExplainDataQuery(R"(
            PRAGMA Kikimr.UseNewEngine = "true";
            DECLARE $value as Utf8;
            SELECT $value as value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(FullOuterJoin, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        CreateSampleTables(kikimr);

        TStreamExecScanQuerySettings settings;
        settings.Explain(true);
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT l.Key, l.Text, l.Data, r.Value1, r.Value2
            FROM `/Root/EightShard` AS l FULL OUTER JOIN `/Root/FourShard` AS r
            ON l.Key = r.Key
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto join = FindPlanNodeByKv(plan, "Node Type", "FullJoin (JoinDict)");
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "FourShard");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST_TWIN(ReadTableRangesFullScan, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);
        auto db = kikimr.GetTableClient();

        auto session = db.CreateSession().GetValueSync().GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TwoKeys` (
                Key1 Int32,
                Key2 Int32,
                Value Int32,
                PRIMARY KEY (Key1, Key2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `TwoKeys` (Key1, Key2, Value) VALUES
                (1, 1, 1),
                (2, 1, 2),
                (3, 2, 3),
                (4, 2, 4),
                (1000, 100, 5),
                (1001, 101, 6),
                (1002, 102, 7),
                (1003, 103, 8);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TVector<std::pair<TString, TString>> testData = {
            {
                "SELECT * FROM `/Root/TwoKeys`;",
                "TableFullScan"
            },
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Key2 > 101;",
                "Filter-TableFullScan"
            }
        };

        for (auto& data: testData) {
            auto it = db.StreamExecuteScanQuery(data.first, settings).GetValueSync();

            auto res = CollectStreamResult(it);
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            UNIT_ASSERT(res.PlanJson);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);

            auto read = FindPlanNodeByKv(plan, "Node Type", data.second);
            UNIT_ASSERT(read.IsDefined());

            auto rangesKeys = FindPlanNodeByKv(plan, "ReadRangesKeys", "[]");
            UNIT_ASSERT(!rangesKeys.IsDefined());

            auto expected = FindPlanNodeByKv(plan, "ReadRangesExpectedSize", "");
            UNIT_ASSERT(!expected.IsDefined());
        }
    }

    Y_UNIT_TEST_TWIN(ReadTableRanges, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        CreateSampleTables(kikimr);

        TStreamExecScanQuerySettings settings;
        settings.Explain(true);
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue`
            WHERE Key >= 2000 OR Key < 99 + 1;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto read = FindPlanNodeByKv(plan, "Node Type", "TableRangesScan");
        UNIT_ASSERT(read.IsDefined());
        auto keys = FindPlanNodeByKv(plan, "ReadRangesKeys", "[\"Key\"]");
        UNIT_ASSERT(keys.IsDefined());
        auto count = FindPlanNodeByKv(plan, "ReadRangesExpectedSize", "2");
        UNIT_ASSERT(count.IsDefined());
    }

    Y_UNIT_TEST_TWIN(Predicates, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TwoKeys` (
                Key1 Int32,
                Key2 Int32,
                Value Int64,
                PRIMARY KEY (Key1, Key2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `TwoKeys` (Key1, Key2, Value) VALUES
                (1, 1, 1),
                (2, 1, 2),
                (3, 2, 3),
                (4, 2, 4),
                (1000, 100, 5),
                (1001, 101, 6),
                (1002, 102, 7),
                (1003, 103, 8);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TVector<std::pair<TString, TString>> testData = {
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Value > 5 And Value <= 10",
                "item.Value > 5 And item.Value <= 10"
            },
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Key2 < 100 Or Value == 5",
                "item.Key2 < 100 Or item.Value == 5"
            },
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Key2 < 100 And Key2 >= 10 And Value != 5",
                "item.Key2 < 100 And item.Key2 >= 10 And item.Value != 5"
            },
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Key2 < 10 Or Cast(Key2 As Int64) < Value",
                "item.Key2 < 10 Or ..."
            }
        };

        for (const auto& [query, predicate] : testData) {
            auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
            auto res = CollectStreamResult(it);

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            UNIT_ASSERT(res.PlanJson);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);

            auto filter = FindPlanNodeByKv(plan, "Name", "Filter");
            UNIT_ASSERT(filter.IsDefined());
            UNIT_ASSERT_C(filter.GetMapSafe().at("Predicate") == predicate,
                          TStringBuilder() << "For query: " << query
                          << " expected predicate: " << predicate
                          << " but received: " << filter.GetMapSafe().at("Predicate"));
        }
    }

    Y_UNIT_TEST_TWIN(MergeConnection, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue` ORDER BY Key;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);

        Cerr << res.PlanJson << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto merge = FindPlanNodeByKv(
            plan,
            "Node Type",
            "Merge"
        );
        UNIT_ASSERT(merge.IsDefined());
        const auto& sortColumns = merge.GetMapSafe().at("SortColumns").GetArraySafe();
        UNIT_ASSERT(sortColumns.size() == 1);
        UNIT_ASSERT(sortColumns.at(0) == "Key (Asc)");
    }
}

} // namespace NKqp
} // namespace NKikimr


