#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TString SettingValue(const TCoNameValueTuple& setting) {
    if (const auto atom = setting.Value().Maybe<TCoAtom>()) {
        return TString(atom.Cast().Value());
    }
    return TString(setting.Value().Cast<TCoUint32>().Literal().Value());
}

TVector<std::pair<TString, TString>> SettingPairs(const TVector<TCoNameValueTuple>& settings) {
    TVector<std::pair<TString, TString>> pairs;
    pairs.reserve(settings.size());
    for (const auto& setting : settings) {
        pairs.emplace_back(TString(setting.Name().Value()), SettingValue(setting));
    }
    return pairs;
}

} // namespace

Y_UNIT_TEST_SUITE(DqOptEqualNulls) {

Y_UNIT_TEST(DisabledByDefault) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    UNIT_ASSERT(BuildBlockHashJoinSettings(pos, EJoinAlgoType::GraceJoin, 2, ctx).empty());
}

Y_UNIT_TEST(EnabledEmitsAllKeyIndexes) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    const auto settings = BuildBlockHashJoinSettings(pos, EJoinAlgoType::GraceJoin, 2, ctx, true);
    UNIT_ASSERT_VALUES_EQUAL(SettingPairs(settings), (TVector<std::pair<TString, TString>>{
        {"EqualNulls", "0"},
        {"EqualNulls", "1"},
    }));
    for (const auto& setting : settings) {
        UNIT_ASSERT(setting.Value().Maybe<TCoUint32>());
    }
}

Y_UNIT_TEST(ReverseJoinKeepsBuildSide) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    UNIT_ASSERT_VALUES_EQUAL(
        SettingPairs(BuildBlockHashJoinSettings(pos, EJoinAlgoType::ReverseBlockJoin, 1, ctx, true)),
        (TVector<std::pair<TString, TString>>{{"BuildSide", "Left"}, {"EqualNulls", "0"}}));
}

Y_UNIT_TEST(PhyBlockHashJoinAstUsesUint32) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    const auto dummy = Build<TCoVoid>(ctx, pos).Done();
    const auto dummyList = Build<TCoAtomList>(ctx, pos).Done();
    const auto settings = BuildBlockHashJoinSettings(pos, EJoinAlgoType::GraceJoin, 2, ctx, true);

    const auto phyJoin = Build<TDqPhyBlockHashJoin>(ctx, pos)
        .LeftInput(dummy)
        .RightInput(dummy)
        .LeftLabel(ctx.NewAtom(pos, "L"))
        .RightLabel(ctx.NewAtom(pos, "R"))
        .JoinType().Build("Inner")
        .JoinKeys(Build<TDqJoinKeyTupleList>(ctx, pos).Done())
        .LeftJoinKeyNames(dummyList)
        .RightJoinKeyNames(dummyList)
        .Settings()
            .Add(settings)
            .Build()
        .Done();

    const auto ast = NCommon::ExprToPrettyString(ctx, phyJoin.Ref());
    UNIT_ASSERT_C(ast.Contains("DqPhyBlockHashJoin"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" (Uint32 '"0"))"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" (Uint32 '"1"))"), ast);
}

} // DqOptEqualNulls
