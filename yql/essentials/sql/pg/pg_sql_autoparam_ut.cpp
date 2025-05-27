#include "ut/util.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslation;

Y_UNIT_TEST_SUITE(PgSqlParsingAutoparam) {
    Y_UNIT_TEST(AutoParamValues_DisabledByDefault) {
        auto res = PgSqlToYql("insert into plato.Output values (1,2,3), (1,2,3)");
        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        auto params = dynamic_cast<TTestAutoParamBuilder*>(res.PgAutoParamValues.Get());
        UNIT_ASSERT_C(!params, "Expected no auto parametrization");
    }

    Y_UNIT_TEST(AutoParamValues_NoParametersWithDefaults) {
        TTranslationSettings settings;
        settings.AutoParametrizeEnabled = true;
        settings.AutoParametrizeValuesStmt = true;
        auto res = SqlToYqlWithMode(
            R"(CREATE TABLE t (a int PRIMARY KEY, b int DEFAULT 0))",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);

        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        auto params = dynamic_cast<TTestAutoParamBuilder*>(res.PgAutoParamValues.Get());
        UNIT_ASSERT_C(!params, "Expected no auto parametrization");
    }

    using TUsedParamsGetter = std::function<void(TSet<TString>&, const NYql::TAstNode& node)>;

    void GetUsedParamsInValues(TSet<TString>& usedParams, const NYql::TAstNode& node) {
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
                continue;
            }

            const auto* option = pgOption->GetChild(1);
            const auto* optionName = option->GetChild(0);

            const bool isValuesNode =
                optionName->IsListOfSize(2) && optionName->GetChild(0)->IsAtom()
                && optionName->GetChild(0)->GetContent() == "quote"
                && optionName->GetChild(1)->GetContent() == "values";
            if (!isValuesNode) {
                continue;
            }
            const auto values = option->GetChild(2);
            if (values->IsAtom()) {
                usedParams.insert(TString(values->GetContent()));
            }
        }
    }

    void TestAutoParam(const TString& query, const THashMap<TString, TString>& expectedParamNameToValue, const TMap<TString, TString>& expectedParamTypes, TUsedParamsGetter usedParamsGetter) {
        TTranslationSettings settings;
        settings.AutoParametrizeEnabled = true;
        settings.AutoParametrizeValuesStmt = true;
        auto res = SqlToYqlWithMode(
            query,
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        auto params = dynamic_cast<TTestAutoParamBuilder*>(res.PgAutoParamValues.Get());
        UNIT_ASSERT_C(params && params->Size() > 0, "Expected auto param values");

        TSet<TString> declaredParams;
        TMap<TString, TString> actualParamTypes;
        VisitAstNodes(*res.Root, [&declaredParams, &actualParamTypes] (const NYql::TAstNode& node) {
            const bool isDeclareNode =
                node.IsList() && node.GetChildrenCount() > 0
                && node.GetChild(0)->IsAtom()
                && node.GetChild(0)->GetContent() == "declare";
            if (isDeclareNode) {
                UNIT_ASSERT_VALUES_EQUAL(node.GetChildrenCount(), 3);
                const auto name = TString(node.GetChild(1)->GetContent());
                declaredParams.insert(name);
                actualParamTypes[name] = node.GetChild(2)->ToString();
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(expectedParamTypes, actualParamTypes);

        TSet<TString> usedParams;
        VisitAstNodes(*res.Root, [&usedParams, &usedParamsGetter] (const auto& node) { return usedParamsGetter(usedParams, node); });
        UNIT_ASSERT_VALUES_EQUAL(declaredParams, usedParams);

        TSet<TString> expectedParams;
        for (auto& [expectedParamName, val] : expectedParamNameToValue) {
            UNIT_ASSERT_C(params->State.contains(expectedParamName),
                "Autoparametrized values do not contain expected param: " << expectedParamName);

            UNIT_ASSERT_STRINGS_EQUAL(val, params->GetParamValue(expectedParamName));
            UNIT_ASSERT_C(declaredParams.contains(expectedParamName),
                "Declared params don't contain expected param name: " << expectedParamName);

            expectedParams.insert(expectedParamName);
        }

        UNIT_ASSERT_VALUES_EQUAL(declaredParams.size(), expectedParams.size());
    }

    Y_UNIT_TEST(AutoParamValues_Int4) {
        TString query = R"(insert into plato.Output values (1,2), (3,4), (4,5))";
        TString expectedParam = "[int4,int4:1,2,3,4,4,5]";
        TString type = "(ListType (TupleType (PgType 'int4) (PgType 'int4)))";
        TestAutoParam(query, {{"a0", expectedParam}}, {{"a0", type}}, GetUsedParamsInValues);
    }

    Y_UNIT_TEST(AutoParamValues_Int4Text) {
        TString query = R"(insert into plato.Output values (1,'2'), (3,'4'))";
        TString expectedParam = "[int4,unknown:1,2,3,4]";
        TString type = "(ListType (TupleType (PgType 'int4) (PgType 'unknown)))";
        TestAutoParam(query, {{"a0", expectedParam}}, {{"a0", type}}, GetUsedParamsInValues);
    }

    Y_UNIT_TEST(AutoParamValues_MultipleStmts) {
        TString query = R"(
            insert into plato.Output values (1,'2'), (3,'4');
            insert into plato.Output1 values (1.23);
        )";
        TString expectedParam0 = "[int4,unknown:1,2,3,4]";
        TString expectedParam1 = "[numeric:1.23]";
        TMap<TString, TString> expectedParamTypes {
            {"a0", "(ListType (TupleType (PgType 'int4) (PgType 'unknown)))"},
            {"a1", "(ListType (TupleType (PgType 'numeric)))"}
        };
        TestAutoParam(query, {{"a0", expectedParam0}, {"a1", expectedParam1}}, expectedParamTypes, GetUsedParamsInValues);
    }

    Y_UNIT_TEST(AutoParamValues_WithNull) {
        TString query = R"(
           insert into plato.Output values (null, '2'), (3, '4')
        )";
        TString expectedParam = "[int4,unknown:#,2,3,4]";
        TString type = "(ListType (TupleType (PgType 'int4) (PgType 'unknown)))";
        TestAutoParam(query, {{"a0", expectedParam}}, {{"a0", type}}, GetUsedParamsInValues);
    }

    Y_UNIT_TEST(AutoParamValues_NullCol) {
        TString query = R"(
            insert into plato.Output values (null,1), (null,1)
        )";
        TString expectedParam = "[unknown,int4:#,1,#,1]";
        TString type = "(ListType (TupleType (PgType 'unknown) (PgType 'int4)))";
        TestAutoParam(query, {{"a0", expectedParam}}, {{"a0", type}}, GetUsedParamsInValues);
    }

    Y_UNIT_TEST(AutoParamConsts_Where) {
        TString query = R"(
            select * from plato.Output where key > 1
        )";
        TString expectedParam = "int4:1";

        // We expect: (PgOp '">" (PgColumnRef '"key") a0)
        const TUsedParamsGetter usedInWhereComp = [] (TSet<TString>& usedParams, const NYql::TAstNode& node) {
            const auto maybeQuote = MaybeGetQuotedValue(node);
            if (!maybeQuote) {
                return;
            }
            const auto quotedVal = maybeQuote.GetRef();
            const bool isWhere =
                quotedVal->IsListOfSize(2) && quotedVal->GetChild(1)->IsListOfSize(3)
                && quotedVal->GetChild(1)->IsListOfSize(3)
                && quotedVal->GetChild(1)->GetChild(0)->IsAtom()
                && quotedVal->GetChild(1)->GetChild(0)->GetContent() == "PgWhere";

            if (!isWhere) {
                return;
            }
            const auto* whereCallable = quotedVal->GetChild(1);

            const auto* whereLambda = whereCallable->GetChild(2);
            const auto* pgOp = whereLambda->GetChild(2);
            const bool isBinaryOp = pgOp->IsListOfSize(4);
            if (!isBinaryOp) {
                return;
            }
            const auto* pgBinOpSecondArg = pgOp->GetChild(3);
            usedParams.insert(TString(pgBinOpSecondArg->GetContent()));
        };
        TString type = "(PgType 'int4)";
        TestAutoParam(query, {{"a0", expectedParam}}, {{"a0", type}}, usedInWhereComp);
    }

    Y_UNIT_TEST(AutoParamConsts_Select) {
        TString query = R"(
            select 1, 'test', B'10001'
        )";
        TString expectedParamInt4 = "int4:1";
        TString expectedParamText = "unknown:test";
        TString expectedParamBit = "bit:b10001";
        const TUsedParamsGetter dummyGetter = [] (TSet<TString>& usedParams, const NYql::TAstNode&) {
            usedParams = {"a0", "a1", "a2"};
        };
        TMap<TString, TString> expectedParamTypes {
            {"a0", "(PgType 'int4)"},
            {"a1", "(PgType 'unknown)"},
            {"a2", "(PgType 'bit)"},
        };
        TestAutoParam(query, {
            {"a0", expectedParamInt4},
            {"a1", expectedParamText},
            {"a2", expectedParamBit},
            }, expectedParamTypes, dummyGetter);
    }

    Y_UNIT_TEST(AutoParamValues_FailToInferColumnType) {
        const auto query = R"(INSERT INTO test VALUES (1), ('2');)";
        TMap<TString, TString> paramToType = {{"a0", "(PgType 'int4)"}, {"a1", "(PgType 'unknown)"}};
        TString expectedParamInt4 = "int4:1";
        TString expectedParamText = "unknown:2";
        const TUsedParamsGetter dummyGetter = [] (TSet<TString>& usedParams, const NYql::TAstNode&) {
            usedParams = {"a0", "a1"};
        };
        TestAutoParam(query, {{"a0", expectedParamInt4}, {"a1", expectedParamText}}, paramToType, dummyGetter);
    }

}
