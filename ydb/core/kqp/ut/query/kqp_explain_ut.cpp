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

    AssertSuccessResult(session.ExecuteSchemeQuery(R"(
        --!syntax_v1

        CREATE TABLE `/Root/test_table_idx_idx` (
            str_field String,
            complex_field Uint64,
            id Uint64,
            PRIMARY KEY (str_field, complex_field)
        );

        CREATE TABLE `/Root/test_table_idx` (
            id Uint64,
            complex_field Uint64,
            str_field String,
            Value String,
            PRIMARY KEY (id)
        );
    )").GetValueSync());

    session.Close();
}

}

Y_UNIT_TEST_SUITE(KqpExplain) {

    Y_UNIT_TEST(Explain) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter");
        if (!join.IsDefined()) {
            join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan");
        }
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "KeyValue");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST(ExplainStream) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter");
        if (!join.IsDefined()) {
            join = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan");
        }
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "KeyValue");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST(ExplainScanQueryWithParams) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            --SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
            DECLARE $value as Utf8;
            SELECT $value as value;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    Y_UNIT_TEST(AggGroupLimit) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

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
            "{_yql_agg_0: MAX(item.Message,state._yql_agg_0),_yql_agg_1: MIN(item.Message,state._yql_agg_1)}");
    }

    Y_UNIT_TEST(ComplexJoin) {
        auto kikimr = DefaultKikimrRunner();
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
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto join = FindPlanNodeByKv(
            plan,
            "Node Type",
            "Aggregate-InnerJoin (MapJoin)-Filter"
        );
        if (!join.IsDefined()) {
            join = FindPlanNodeByKv(
                plan,
                "Node Type",
                "Aggregate-InnerJoin (MapJoin)-Filter-TableFullScan"
            );
        }
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "FourShard");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST(PrecomputeRange) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableRangesScan");
        if (!node.IsDefined()) {
            node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableRangeScan");
        }
        UNIT_ASSERT(node.IsDefined());

        auto operators = node.GetMapSafe().at("Operators").GetArraySafe();
        if (operators[1].GetMapSafe().at("Name") == "TableRangesScan") {
            auto& readRanges = operators[1].GetMapSafe().at("ReadRanges").GetArraySafe();
            UNIT_ASSERT(readRanges[0] == "Key [150, 266]");
        } else {
            UNIT_ASSERT(operators[1].GetMapSafe().at("Name") == "TableRangeScan");
        }
    }

    Y_UNIT_TEST(CompoundKeyRange) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/Logs` WHERE App = "new_app_1" AND Host < "xyz" AND Ts = (42+7) Limit 10;
        )", settings).GetValueSync();

        auto res = CollectStreamResult(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(res.PlanJson);
        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto read = FindPlanNodeByKv(plan, "Node Type", "Limit-TableRangeScan");
        size_t operatorsCount = 2;
        size_t lookupMember = 1;
        if (!read.IsDefined()) {
            read = FindPlanNodeByKv(plan, "Node Type", "Limit-Filter-TableRangeScan");
            operatorsCount = 3;
            lookupMember = 2;
        }
        auto& operators = read.GetMapSafe().at("Operators").GetArraySafe();
        UNIT_ASSERT(operators.size() == operatorsCount);

        auto& rangeRead = operators[lookupMember].GetMapSafe();
        UNIT_ASSERT(rangeRead.at("Name") == "TableRangeScan");
        UNIT_ASSERT_VALUES_EQUAL(rangeRead.at("ReadRange").GetArraySafe()[0], "App («new_app_1»)");
    }

    Y_UNIT_TEST(SortStage) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto scanSort = FindPlanNodeByKv(plan, "Node Type", "Sort-TableRangeScan");
        if (!scanSort.IsDefined()) {
            scanSort = FindPlanNodeByKv(plan, "Node Type", "Sort-Filter-TableRangeScan");
        }
        UNIT_ASSERT(scanSort.IsDefined());
    }

    Y_UNIT_TEST(LimitOffset) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto limit = FindPlanNodeByKv(plan, "Limit", "10");
        UNIT_ASSERT(limit.IsDefined());
        auto offset = FindPlanNodeByKv(plan, "Offset", "15");
        UNIT_ASSERT(offset.IsDefined());
    }

    Y_UNIT_TEST(SelfJoin3xSameLabels) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);

        TKikimrRunner kikimr(app);
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto join1 = FindPlanNodeByKv(plan, "Node Type", "Sort-InnerJoin (MapJoin)-Filter");
        UNIT_ASSERT(join1.IsDefined());
        auto join2 = FindPlanNodeByKv(plan, "Node Type", "Aggregate-InnerJoin (MapJoin)-Filter");
        UNIT_ASSERT(join2.IsDefined());
    }

    Y_UNIT_TEST(PureExpr) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto constExpr = FindPlanNodeByKv(plan, "Node Type", "ConstantExpr");
        UNIT_ASSERT(constExpr.IsDefined());
    }

    Y_UNIT_TEST(MultiUsedStage) {
        NKikimrConfig::TAppConfig appCfg;
        auto* spilling = appCfg.MutableTableServiceConfig()->MutableSpillingServiceConfig()->MutableLocalFileConfig();
        spilling->SetEnable(true);
        spilling->SetRoot("./spilling/");
        auto kikimr = DefaultKikimrRunner({}, appCfg);

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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

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

    Y_UNIT_TEST(SqlIn) {
        auto kikimr = DefaultKikimrRunner();
        CreateSampleTables(kikimr);

        TStreamExecScanQuerySettings settings;
        settings.Explain(true);
        auto db = kikimr.GetTableClient();

        auto query = R"(
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto unionNode = FindPlanNodeByKv(plan, "Node Type", "Sort-Union");
        if (unionNode.IsDefined()) {
            UNIT_ASSERT_EQUAL(unionNode.GetMap().at("Plans").GetArraySafe().size(), 4);
        } else {
            UNIT_ASSERT(FindPlanNodeByKv(plan, "Node Type", "Merge").IsDefined());
        }
    }

    Y_UNIT_TEST(ExplainDataQuery) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 42) ORDER BY Key;
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 4*2) ORDER BY Key;
            SELECT count(distinct Value) FROM `/Root/KeyValue` WHERE Key > 20 and Key <= 120;
            SELECT count(distinct Value) FROM `/Root/KeyValue`;
            SELECT count(distinct Value) FROM `/Root/KeyValue` WHERE Key >= 10;
        )").ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        Cerr << "Plan " << result.GetPlan() << Endl;

        auto node = FindPlanNodeByKv(plan, "Name", "TableRangeScan");
        UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");
        node = FindPlanNodeByKv(plan, "Name", "TableFullScan");
        UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");


        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            node = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
        } else {
            node = FindPlanNodeByKv(plan, "Name", "TablePointLookup");
        }

        if (node.IsDefined()) {
            UNIT_ASSERT_EQUAL(node.GetMapSafe().at("Table").GetStringSafe(), "KeyValue");
        }
    }

    Y_UNIT_TEST(FewEffects) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data=Data+1;
            UPDATE `/Root/EightShard` SET Data=Data-1 WHERE Key In (100,200,300);
            DELETE FROM `/Root/EightShard` WHERE Key > 350;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        Cerr << plan << Endl;

        auto upsertsCount = CountPlanNodesByKv(plan, "Node Type", "Upsert-ConstantExpr");
        UNIT_ASSERT_VALUES_EQUAL(upsertsCount, 2);

        auto deletesCount = CountPlanNodesByKv(plan, "Node Type", "Delete-ConstantExpr");
        UNIT_ASSERT_VALUES_EQUAL(deletesCount, 1);

        auto fullScansCount = CountPlanNodesByKv(plan, "Node Type", "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL(fullScansCount, 1);

        auto rangeScansCount = CountPlanNodesByKv(plan, "Node Type", "TableRangeScan");
        UNIT_ASSERT_VALUES_EQUAL(rangeScansCount, 2);

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
    }

    Y_UNIT_TEST(ExplainDataQueryWithParams) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            DECLARE $value as Utf8;
            SELECT $value as value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto result2 = session.ExplainDataQuery(R"(
            DECLARE $value as Utf8;
            SELECT $value as value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
    }

    Y_UNIT_TEST(FullOuterJoin) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto join = FindPlanNodeByKv(plan, "Node Type", "FullJoin (JoinDict)");
        UNIT_ASSERT(join.IsDefined());
        auto left = FindPlanNodeByKv(join, "Table", "EightShard");
        UNIT_ASSERT(left.IsDefined());
        auto right = FindPlanNodeByKv(join, "Table", "FourShard");
        UNIT_ASSERT(right.IsDefined());
    }

    Y_UNIT_TEST(ReadTableRangesFullScan) {
        auto kikimr = DefaultKikimrRunner();
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

        TVector<std::pair<TString, TVector<TString>>> testData = {
            {
                "SELECT * FROM `/Root/TwoKeys`;",
                {"TableFullScan", "Collect-TableFullScan"}
            },
            {
                "SELECT * FROM `/Root/TwoKeys` WHERE Key2 > 101;",
                {"Filter-TableFullScan"}
            }
        };

        for (auto& data: testData) {
            auto it = db.StreamExecuteScanQuery(data.first, settings).GetValueSync();

            auto res = CollectStreamResult(it);
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            UNIT_ASSERT(res.PlanJson);

            Cerr << *res.PlanJson << Endl;

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);
            UNIT_ASSERT(ValidatePlanNodeIds(plan));

            auto read = FindPlanNodeByKv(plan, "Node Type", data.second[0]);
            if (!read.IsDefined()) {
                size_t i = 1;
                while (!read.IsDefined() && i < data.second.size()) {
                    read = FindPlanNodeByKv(plan, "Node Type", data.second[i]);
                }
            }
            UNIT_ASSERT(read.IsDefined());

            auto rangesKeys = FindPlanNodeByKv(plan, "ReadRangesKeys", "[]");
            UNIT_ASSERT(!rangesKeys.IsDefined());

            auto expected = FindPlanNodeByKv(plan, "ReadRangesExpectedSize", "");
            UNIT_ASSERT(!expected.IsDefined());
        }
    }

    Y_UNIT_TEST(ReadTableRanges) {
        auto kikimr = DefaultKikimrRunner();
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

        Cerr << *res.PlanJson << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto read = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
        if (!read.IsDefined()) {
            read = FindPlanNodeByKv(plan, "Name", "TableRangeScan");
        }
        UNIT_ASSERT(read.IsDefined());
        auto keys = FindPlanNodeByKv(plan, "ReadRangesKeys", "[\"Key\"]");
        UNIT_ASSERT(keys.IsDefined());
        auto count = FindPlanNodeByKv(plan, "ReadRangesExpectedSize", "2");
        UNIT_ASSERT(count.IsDefined());
    }

    Y_UNIT_TEST(Predicates) {
        auto kikimr = DefaultKikimrRunner();
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
                "item.Key2 < 10 Or item.Key2 < item.Value"
            }
        };

        for (const auto& [query, predicate] : testData) {
            auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
            auto res = CollectStreamResult(it);

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            UNIT_ASSERT(res.PlanJson);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);
            UNIT_ASSERT(ValidatePlanNodeIds(plan));

            auto filter = FindPlanNodeByKv(plan, "Name", "Filter");
            UNIT_ASSERT(filter.IsDefined());
            UNIT_ASSERT_C(filter.GetMapSafe().at("Predicate") == predicate,
                          TStringBuilder() << "For query: " << query
                          << " expected predicate: " << predicate
                          << " but received: " << filter.GetMapSafe().at("Predicate"));
        }
    }

    Y_UNIT_TEST(MergeConnection) {
        auto kikimr = DefaultKikimrRunner();
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
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

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

    Y_UNIT_TEST(SsaProgramInJsonPlan) {
        auto kikimr = DefaultKikimrRunner();
        TStreamExecScanQuerySettings settings;
        settings.Explain(true);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/OlapTable` (
                Key Int32 NOT NULL,
                Value Int64,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        TString query = "SELECT * FROM `/Root/OlapTable` WHERE Value > 0;";
        auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
        auto streamRes = CollectStreamResult(it);

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        UNIT_ASSERT(streamRes.PlanJson);

        Cerr << streamRes.PlanJson.GetOrElse("NO_PLAN") << Endl;
        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*streamRes.PlanJson, &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto readNode = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
        UNIT_ASSERT(readNode.IsDefined());

        auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
        for (auto& op : operators) {
            if (op.GetMapSafe().at("Name") == "TableFullScan") {
                UNIT_ASSERT(op.GetMapSafe().at("SsaProgram").IsDefined());
            }
        }
    }

    Y_UNIT_TEST_TWIN(IdxFullscan, Source) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(Source);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        CreateSampleTables(kikimr);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto res = session.ExplainDataQuery(R"(
            SELECT t.*
            FROM
               (SELECT * FROM `/Root/test_table_idx_idx`
               WHERE `str_field` is NULL
               ) as idx
            INNER JOIN
               `/Root/test_table_idx` AS t
            USING (`id`)
        )").GetValueSync();

        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        auto strPlan = res.GetPlan();
        UNIT_ASSERT(strPlan);

        Cerr << strPlan << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(strPlan, &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto fullscan = FindPlanNodeByKv(
            plan,
            "Name",
            "TableFullScan"
       );
        UNIT_ASSERT(!fullscan.IsDefined());
    }

    Y_UNIT_TEST(MultiJoinCteLinks) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(false);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            select * from `/Root/KeyValue` as kv
                inner join `/Root/EightShard` as es on kv.Key == es.Key;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);

        auto cteLink0 = FindPlanNodeByKv(
            plan,
            "CTE Name",
            "precompute_0_0"
        );
        UNIT_ASSERT(cteLink0.IsDefined());

        auto cteLink1 = FindPlanNodeByKv(
            plan,
            "CTE Name",
            "precompute_1_0"
        );

        UNIT_ASSERT(cteLink1.IsDefined());
    }
}

} // namespace NKqp
} // namespace NKikimr


