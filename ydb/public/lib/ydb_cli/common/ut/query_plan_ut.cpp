#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(TQueryPlanPrinterTest) {
    TString Plan(TStringBuf op, TStringBuf connection) {
        NJson::TJsonValue node;
        UNIT_ASSERT(NJson::ReadJsonTree(connection, &node));
        NJson::TJsonValue operation;
        UNIT_ASSERT(NJson::ReadJsonTree(op, &operation));
        operation["A-Cpu"] = 0.5;
        operation["A-Rows"] = 42;
        NJson::TJsonValue stage;
        stage["Node Type"] = "Aggregate";
        stage["Operators"].AppendValue(operation);
        // The aggregate reads recent orders and filtered archive rows.
        NJson::TJsonValue orders;
        UNIT_ASSERT(NJson::ReadJsonTree(R"({
            "Node Type":"UnionAll",
            "Operators":[{"Name":"UnionAll"}],
            "Plans":[
                {
                    "Node Type":"TableRangeScan",
                    "Operators":[{"Name":"TableRangeScan","Table":"orders","E-Rows":0.00042,"E-Size":1000000000}],
                    "Tables":["orders"],
                    "Stats":{"TotalTasks":2}
                },
                {
                    "Node Type":"Filter",
                    "Operators":[{"Name":"Filter","Predicate":"item.status = paid","SsaProgram":{"Program":"Read archive"}}],
                    "Plans":[{
                        "Node Type":"TableRangeScan",
                        "Operators":[{"Name":"TableRangeScan","Table":"archived_orders"}]
                    }]
                }
            ]
        })", &orders));
        stage["Plans"].AppendValue(orders);
        node["Plans"].AppendValue(stage);
        NJson::TJsonValue root;
        root["Node Type"] = "ResultSet";
        root["Plans"].AppendValue(node);
        NJson::TJsonValue plan;
        plan["meta"]["type"] = "query";
        plan["Plan"]["Plans"].AppendValue(root);
        plan["SimplifiedPlan"] = root;
        return NJson::WriteJson(plan, false);
    }

    Y_UNIT_TEST(StructuredAndLegacyProperties) {
        const auto legacy = Plan(R"({
            "Name":"Aggregate","E-Cost":"123.5","E-Rows":"45","E-Size":"2048",
            "Aggregation":"{n: count(distinct a)}","GroupBy":"b, c",
            "SortBy":"b desc nulls last","PartitionBy":"c","LookupKeyPrefix":"a, b",
            "Frame":"Rows[Preceding 2, CurrentRow]"
        })", R"({"Node Type":"Merge","PlanNodeType":"Connection","SortColumns":["b desc nulls last"]})");
        const auto structured = Plan(R"({
            "Name":"Aggregate","E-Cost":123.5,"E-Rows":45,"E-Size":2048,
            "Aggregation":[{"Output":"n","Function":"count","Arguments":["a"],"Distinct":true}],"GroupBy":["b","c"],
            "SortBy":[{"Column":"b","Ascending":false,"NullsFirst":false}],"PartitionBy":["c"],"LookupKeyPrefix":["a","b"],
            "Frame":{"Type":"Rows","Begin":{"Kind":"Preceding","Value":2},"End":{"Kind":"CurrentRow"}}
        })", R"({"Node Type":"Merge","PlanNodeType":"Connection","SortColumns":[{"Column":"b","Ascending":false,"NullsFirst":false}]})");
        for (auto format : {EDataFormat::Pretty, EDataFormat::PrettyTable}) {
            for (const bool analyze : {false, true}) {
                TStringStream expected;
                TQueryPlanPrinter(format, analyze, expected).Print(legacy);
                TStringStream actual;
                TQueryPlanPrinter(format, analyze, actual).Print(structured);
                UNIT_ASSERT_VALUES_EQUAL(actual.Str(), expected.Str());
                UNIT_ASSERT(actual.Str().Contains("count(distinct a)"));
                UNIT_ASSERT(actual.Str().Contains("b desc nulls last"));
                UNIT_ASSERT(actual.Str().Contains("Rows[Preceding 2, CurrentRow]"));
                UNIT_ASSERT(actual.Str().Contains(format == EDataFormat::PrettyTable ? "124" : "123.5"));
                UNIT_ASSERT(actual.Str().Contains("archived_orders"));
                if (format == EDataFormat::PrettyTable) {
                    UNIT_ASSERT(actual.Str().Contains("├──> "));
                    UNIT_ASSERT(actual.Str().Contains("└─┬> "));
                    UNIT_ASSERT(actual.Str().Contains("4.200e-04"));
                    UNIT_ASSERT(actual.Str().Contains("1.000e+09"));
                    UNIT_ASSERT(actual.Str().Contains("status = paid"));
                    UNIT_ASSERT(!actual.Str().Contains("item.status"));
                    UNIT_ASSERT(!actual.Str().Contains("SsaProgram"));
                } else {
                    UNIT_ASSERT(actual.Str().Contains("├──TableRangeScan"));
                    UNIT_ASSERT(actual.Str().Contains("└──Filter"));
                    UNIT_ASSERT_VALUES_EQUAL(actual.Str().Contains("TotalTasks: 2"), analyze);
                }
                if (analyze) {
                    UNIT_ASSERT(actual.Str().Contains("42"));
                }
            }
        }

        TStringStream json;
        TQueryPlanPrinter(EDataFormat::JsonUnicode, false, json).Print(structured);
        NJson::TJsonValue original, printed;
        UNIT_ASSERT(NJson::ReadJsonTree(structured, &original));
        UNIT_ASSERT(NJson::ReadJsonTree(json.Str(), &printed));
        UNIT_ASSERT_VALUES_EQUAL(original, printed);

        TStringStream simplified;
        TQueryPlanPrinter(EDataFormat::JsonBase64Simplify, false, simplified).Print(structured);
        NJson::TJsonValue simplifiedPlan;
        UNIT_ASSERT(NJson::ReadJsonTree(simplified.Str(), &simplifiedPlan));
        UNIT_ASSERT_VALUES_EQUAL(original["SimplifiedPlan"], simplifiedPlan);
    }
}
