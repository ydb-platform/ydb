#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include "kqp_rbo_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>
#include <ydb/core/kqp/opt/rbo/rules/kqp_cbo_trees.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboCboFilters) {
    Y_UNIT_TEST(RestoresPredicatesInBothOptimizers) {
        for (bool newRbo : {false, true}) {
            NKikimrConfig::TAppConfig config;
            config.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
            config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
            auto client = kikimr.GetQueryClient();
            auto execute = [&](const TString& sql, bool schema = false) {
                auto result = client.ExecuteQuery(sql, schema ? NYdb::NQuery::TTxControl::NoTx()
                    : NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), "new RBO=" << newRbo << "\n" << sql << "\n" << result.GetIssues().ToString());
                return result;
            };
            execute(R"(
                CREATE TABLE `/Root/A` (k Int32 NOT NULL, PRIMARY KEY(k));
                CREATE TABLE `/Root/B` (k Int32 NOT NULL, a Int32, x Int32, PRIMARY KEY(k));
                CREATE TABLE `/Root/C` (k Int32 NOT NULL, a Int32, x Int32, PRIMARY KEY(k));
                CREATE TABLE `/Root/F` (k Int32 NOT NULL, PRIMARY KEY(k));
            )", true);
            execute(R"(
                UPSERT INTO `/Root/A` (k) VALUES (1), (2), (3);
                UPSERT INTO `/Root/F` (k) VALUES (1), (2), (3);
                UPSERT INTO `/Root/B` (k, a, x) VALUES (11, 1, 5), (12, 1, 6), (13, 1, 5), (21, 2, NULL);
                UPSERT INTO `/Root/C` (k, a, x) VALUES (101, 1, 5), (102, 1, NULL), (201, 2, NULL), (301, 3, NULL);
            )");
            for (bool equalNulls : {false, true}) {
                // Legacy SQL accepts only equality predicates in JOIN ON.
                if (equalNulls && !newRbo) {
                    continue;
                }
                const TString comparison = equalNulls ? "IS NOT DISTINCT FROM" : "=";
                const TString joins = TStringBuilder() << R"(
                    SELECT a.k AS id FROM `/Root/A` AS a
                    LEFT JOIN `/Root/B` AS b ON a.k = b.a
                    INNER JOIN `/Root/C` AS c ON a.k = c.a AND b.x )" << comparison << " c.x";
                const TString pragmas = TStringBuilder()
                    << (newRbo ? "PRAGMA YqlSelect = 'force';\n" : "")
                    << "PRAGMA ydb.CostBasedOptimizationLevel = '4';\n"
                    << "PRAGMA ydb.OptimizerHints = 'JoinOrder((a c) b)';\n";
                auto result = execute(pragmas + joins + " ORDER BY id;");
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                    equalNulls ? "[[1];[1];[2];[3]]" : "[[1];[1]]");

                // The residual must run inside the nullable input of F LEFT JOIN.
                // Moving it to the top would discard F's otherwise unmatched rows.
                result = execute(pragmas + "$q = (" + joins + ");\n" + R"(
                    SELECT f.k, q.id FROM `/Root/F` AS f LEFT JOIN $q AS q ON f.k = q.id ORDER BY f.k;
                )");
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), equalNulls
                    ? "[[1;[1]];[1;[1]];[2;[2]];[3;[3]]]" : "[[1;[1]];[1;[1]];[2;#];[3;#]]");
            }
        }
    }

    Y_UNIT_TEST(RebuildPostJoinConditionsAsFilters) {
        for (bool equalNulls : {false, true}) {
            for (bool multiple : {false, true}) {
                NTests::TIdTestContext f;
                const auto a = f.Id("a"), b = f.Id("b"), bx = f.Id("bx"), c = f.Id("c"), cx = f.Id("cx");
                auto relation = [&](const TString& name, TUnorderedIUs columns) {
                    return std::make_shared<NOpt::TRBORelOptimizerNode>(
                        TVector<TString>{name}, TOptimizerStatistics{}, f.Read(std::move(columns)));
                };
                auto ac = std::make_shared<TJoinOptimizerNode>(relation("A", {a}), relation("C", {c, cx}),
                    TVector<TJoinColumn>{MakeCBOColumn("A", a)}, TVector<TJoinColumn>{MakeCBOColumn("C", c)},
                    EJoinKind::InnerJoin, EJoinAlgoType::GraceJoin, false, false);
                auto acb = std::make_shared<TJoinOptimizerNode>(ac, relation("B", {b, bx}),
                    TVector<TJoinColumn>{MakeCBOColumn("A", a)}, TVector<TJoinColumn>{MakeCBOColumn("B", b)},
                    EJoinKind::LeftJoin, EJoinAlgoType::GraceJoin, false, false);
                auto left = MakeCBOColumn("B", bx), right = MakeCBOColumn("C", cx);
                left.EqualNulls = right.EqualNulls = equalNulls;
                acb->PostJoinConditions.emplace_back(left, right);
                if (multiple) {
                    left = MakeCBOColumn("B", b);
                    right = MakeCBOColumn("C", c);
                    left.EqualNulls = right.EqualNulls = equalNulls;
                    acb->PostJoinConditions.emplace_back(left, right);
                }
                acb->ShuffleLeftSideBy = {MakeCBOColumn("A", a)};
                acb->ShuffleRightSideBy = {MakeCBOColumn("B", b)};

                auto converted = ConvertOptimizedTree(acb, {}, f.Pos, f.ExprCtx, f.Props);
                UNIT_ASSERT(converted->Kind == EOperator::Filter);
                const auto filter = CastOperator<TOpFilter>(converted);
                const auto join = CastOperator<TOpJoin>(filter->GetInput());
                UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, "Left");
                UNIT_ASSERT_VALUES_EQUAL(join->JoinKeys.Items().size(), 1);
                UNIT_ASSERT(join->JoinFilters.empty());
                UNIT_ASSERT(join->Props.JoinAlgo == EJoinAlgoType::GraceJoin);
                UNIT_ASSERT(join->Props.LeftShuffleBy == TOrderedIUs<>{a});
                UNIT_ASSERT(join->Props.RightShuffleBy == TOrderedIUs<>{b});
                const auto predicate = filter->GetFilterExpression();
                UNIT_ASSERT(predicate.GetInputIUs() == (multiple ? TUnorderedIUs({b, bx, c, cx}) : TUnorderedIUs({bx, cx})));
                const auto conjuncts = predicate.SplitConjunct();
                UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), multiple ? 2 : 1);
                for (const auto& conjunct : conjuncts) {
                    UNIT_ASSERT(conjunct.GetExpressionBody()->IsCallable(equalNulls ? "IsNotDistinctFrom" : "=="));
                }

                auto root = f.Root(converted, {});
                NTests::AssertIdInvariants(*root->GetInput(), root->PlanProps);
                root->MutableChild(0) = TPushFilterIntoJoinRule().SimpleMatchAndApply(
                    root->GetInput(), f.RboCtx, root->PlanProps);
                root->RecomputeOutputIUsSubtree();
                // Only strict equality may reduce the LEFT join and become an ON key.
                UNIT_ASSERT(root->GetInput()->Kind == (equalNulls ? EOperator::Filter : EOperator::Join));
                UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, equalNulls ? "Left" : "Inner");
                NTests::AssertIdInvariants(*root->GetInput(), root->PlanProps);
            }
        }
    }
}

} // namespace NKikimr::NKqp
