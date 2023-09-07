#include "ut/util.h"

#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslation;

Y_UNIT_TEST_SUITE(PgSqlParsingAutoparam) {
    Y_UNIT_TEST(AutoParamStmt_DisabledByDefault) {
        auto res = PgSqlToYql("insert into plato.Output values (1,2,3), (1,2,3)");
        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        UNIT_ASSERT_C(res.PgAutoParamValues.Empty(), "Expected no auto parametrization");
    }
    
    Y_UNIT_TEST(AutoParamStmt_DifferentTypes) {
        TTranslationSettings settings;
        settings.AutoParametrizeEnabled = true;
        auto res = SqlToYqlWithMode(
            R"(insert into plato.Output values (1,2,3), (1,'2',3))",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);

        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        UNIT_ASSERT_C(res.PgAutoParamValues.Empty(), "Expected no auto parametrization");
    }

    void TestAutoParam(const TString& query, const THashMap<TString, TString>& expectedParamNameToJsonYdbVal) {
        TTranslationSettings settings;
        settings.AutoParametrizeEnabled = true;
        auto res = SqlToYqlWithMode(
            query,
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        UNIT_ASSERT_C(res.PgAutoParamValues && !res.PgAutoParamValues->empty(), "Expected auto param values");

        TSet<TString> declaredParams;
        VisitAstNodes(*res.Root, [&declaredParams] (const NYql::TAstNode& node) {
            const bool isDeclareNode = 
                node.IsList() && node.GetChildrenCount() > 0 
                && node.GetChild(0)->IsAtom() 
                && node.GetChild(0)->GetContent() == "declare";
            if (isDeclareNode) {
                UNIT_ASSERT_VALUES_EQUAL(node.GetChildrenCount(), 3);
                declaredParams.insert(TString(node.GetChild(1)->GetContent()));
            }
        });

        TSet<TString> usedParams;
        VisitAstNodes(*res.Root, [&usedParams] (const NYql::TAstNode& node) {
            const bool isPgSetItem = 
                node.IsListOfSize(2) && node.GetChild(0)->IsAtom() 
                && node.GetChild(0)->GetContent() == "PgSetItem";
            if (!isPgSetItem) {
                return;
            }
            const auto pgSetItemOptions = node.GetChild(1)->GetChild(1);

            for (const auto* pgOption : pgSetItemOptions->GetChildren()) {
                const bool isQuotedList = 
                    pgOption->IsListOfSize(2) && pgOption->GetChild(0)->IsAtom() 
                    && pgOption->GetChild(0)->GetContent() == "quote";
                if (!isQuotedList) {
                    return;
                }

                const auto* option = pgOption->GetChild(1);
                const auto& optionName = option->GetChild(0);

                const bool isValuesNode = 
                    optionName->IsListOfSize(2) && optionName->GetChild(0)->IsAtom()
                    && optionName->GetChild(0)->GetContent() == "quote"
                    && optionName->GetChild(1)->GetContent() == "values";
                if (!isValuesNode) {
                    return;
                }
                const auto values = option->GetChild(2);
                if (values->IsAtom()) {
                    usedParams.insert(TString(values->GetContent()));
                }
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(declaredParams, usedParams);

        TMap<TString, Ydb::TypedValue> expectedParams;
        for (auto& [expectedParamName, jsonValue] : expectedParamNameToJsonYdbVal) {
            Ydb::TypedValue expectedParam;
            google::protobuf::util::JsonStringToMessage(jsonValue, &expectedParam);
            expectedParams.emplace(expectedParamName, expectedParam);
            UNIT_ASSERT_C(res.PgAutoParamValues->contains(expectedParamName),
                "Autoparametrized values do not contain expected param: " << expectedParamName);

            auto actualParam = res.PgAutoParamValues.GetRef()[expectedParamName];
            UNIT_ASSERT_STRINGS_EQUAL(expectedParam.ShortUtf8DebugString(), actualParam.ShortUtf8DebugString());
            UNIT_ASSERT_C(declaredParams.contains(expectedParamName), 
                "Declared params don't contain expected param name: " << expectedParamName);
        }

        UNIT_ASSERT_VALUES_EQUAL(declaredParams.size(), expectedParams.size());
    }

    Y_UNIT_TEST(AutoParamStmt_Int4) {
        TString query = R"(insert into plato.Output values (1,2), (3,4), (4,5))";
        TString expectedParamJson = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":23}},{"pg_type":{"oid":23}}]}}}},
            "value":{"items":[{"items":[{"text_value":"1"},{"text_value":"2"}]},{"items":[{"text_value":"3"},{"text_value":"4"}]},{"items":[{"text_value":"4"},{"text_value":"5"}]}]}}
        )";
        TestAutoParam(query, {{"a0", expectedParamJson}});
    }

    Y_UNIT_TEST(AutoParamStmt_Int4Text) {
        TString query = R"(insert into plato.Output values (1,'2'), (3,'4'))";
        TString expectedParamJson = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":23}},{"pg_type":{"oid":705}}]}}}},
            "value":{"items":[{"items":[{"text_value":"1"},{"text_value":"2"}]},{"items":[{"text_value":"3"},{"text_value":"4"}]}]}}
        )";
        TestAutoParam(query, {{"a0", expectedParamJson}});
    }

    Y_UNIT_TEST(AutoParamStmt_MultipleStmts) {
        TString query = R"(
            insert into plato.Output values (1,'2'), (3,'4');
            insert into plato.Output1 values (1.23);
        )";
        TString expectedParamJson0 = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":23}},{"pg_type":{"oid":705}}]}}}},
            "value":{"items":[{"items":[{"text_value":"1"},{"text_value":"2"}]},{"items":[{"text_value":"3"},{"text_value":"4"}]}]}}
        )";
        TString expectedParamJson1 = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":1700}}]}}}},
            "value":{"items":[{"items":[{"text_value":"1.23"}]}]}}
        )";
        TestAutoParam(query, {{"a0", expectedParamJson0}, {"a1", expectedParamJson1}});
    }

    Y_UNIT_TEST(AutoParamStmt_WithNull) {
        TString query = R"(
           insert into plato.Output values (null, '2'), (3, '4')
        )";
        TString expectedParamJson = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":23}},{"pg_type":{"oid":705}}]}}}},
            "value":{"items":[{"items":[{"null_flag_value":"NULL_VALUE"},{"text_value":"2"}]},{"items":[{"text_value":"3"},{"text_value":"4"}]}]}}
        )";
        TestAutoParam(query, {{"a0", expectedParamJson}});
    }

    Y_UNIT_TEST(AutoParamStmt_NullCol) {
        TString query = R"(
            insert into plato.Output values (null,1), (null,1)
        )";
        TString expectedParamJson = R"(
            {"type":{"list_type":{"item":{"tuple_type":{"elements":[{"pg_type":{"oid":705}},{"pg_type":{"oid":23}}]}}}},
            "value":{"items":[{"items":[{"null_flag_value":"NULL_VALUE"},{"text_value":"1"}]},{"items":[{"null_flag_value":"NULL_VALUE"},{"text_value":"1"}]}]}}
        )";
        TestAutoParam(query, {{"a0", expectedParamJson}});
    }
}
