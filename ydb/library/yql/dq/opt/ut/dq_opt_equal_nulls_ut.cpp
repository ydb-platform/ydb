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

TDqJoin MakeDqJoin(
    TExprContext& ctx,
    const TVector<TString>& flags = {},
    const TVector<std::pair<TString, TString>>& options = {})
{
    const auto pos = ctx.AppendPosition({});
    const auto dummy = Build<TCoVoid>(ctx, pos).Done();
    const auto dummyList = Build<TCoAtomList>(ctx, pos).Done();
    const auto joinKeys = Build<TDqJoinKeyTupleList>(ctx, pos).Done();

    TVector<TCoNameValueTuple> joinAlgoOptions;
    joinAlgoOptions.reserve(options.size());
    for (const auto& [name, value] : options) {
        joinAlgoOptions.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(name)
                .Value<TCoAtom>().Build(value)
                .Done());
    }

    TVector<TCoAtom> flagAtoms;
    flagAtoms.reserve(flags.size());
    for (const auto& flag : flags) {
        flagAtoms.emplace_back(ctx.NewAtom(pos, flag));
    }

    return Build<TDqJoin>(ctx, pos)
        .LeftInput(dummy)
        .RightInput(dummy)
        .LeftLabel(ctx.NewAtom(pos, "L"))
        .RightLabel(ctx.NewAtom(pos, "R"))
        .JoinType().Build("Inner")
        .JoinKeys(joinKeys)
        .LeftJoinKeyNames(dummyList)
        .RightJoinKeyNames(dummyList)
        .JoinAlgo().Build("GraceJoin")
        .ShuffleLeftSideBy()
            .Build()
        .ShuffleRightSideBy()
            .Build()
        .JoinAlgoOptions()
            .Add(std::move(joinAlgoOptions))
            .Build()
        .Flags<TCoAtomList>()
            .Add(flagAtoms)
            .Build()
        .Done();
}

TVector<std::pair<TString, TString>> SettingPairs(const TVector<TCoNameValueTuple>& settings) {
    TVector<std::pair<TString, TString>> pairs;
    pairs.reserve(settings.size());
    for (const auto& setting : settings) {
        pairs.emplace_back(TString(setting.Name().Value()), TString(setting.Value().Cast<TCoAtom>().Value()));
    }
    return pairs;
}

} // namespace

Y_UNIT_TEST_SUITE(DqOptEqualNulls) {

Y_UNIT_TEST(CollectKeysEmptyByDefault) {
    TExprContext ctx;
    const auto join = MakeDqJoin(ctx);
    UNIT_ASSERT(CollectEqualNullsKeys(join, 2).empty());
}

Y_UNIT_TEST(CollectKeysFromFlagAppliesToAllKeys) {
    TExprContext ctx;
    const auto join = MakeDqJoin(ctx, {"EqualNulls"});
    UNIT_ASSERT_VALUES_EQUAL(CollectEqualNullsKeys(join, 3), (TVector<ui32>{0, 1, 2}));
}

Y_UNIT_TEST(CollectKeysFromBoolOption) {
    TExprContext ctx;
    const auto enabled = MakeDqJoin(ctx, {}, {{"EqualNulls", "true"}});
    UNIT_ASSERT_VALUES_EQUAL(CollectEqualNullsKeys(enabled, 2), (TVector<ui32>{0, 1}));

    const auto disabled = MakeDqJoin(ctx, {}, {{"EqualNulls", "false"}});
    UNIT_ASSERT(CollectEqualNullsKeys(disabled, 2).empty());
}

Y_UNIT_TEST(CollectKeysFromIndexOption) {
    TExprContext ctx;
    const auto join = MakeDqJoin(ctx, {}, {{"EqualNulls", "1"}});
    UNIT_ASSERT_VALUES_EQUAL(CollectEqualNullsKeys(join, 2), (TVector<ui32>{1}));
}

Y_UNIT_TEST(BuildSettingsForPhyBlockHashJoin) {
    TExprContext ctx;
    const auto join = MakeDqJoin(ctx, {}, {{"EqualNulls", "0"}});

    UNIT_ASSERT_VALUES_EQUAL(
        SettingPairs(BuildBlockHashJoinSettings(join, EJoinAlgoType::GraceJoin, 1, ctx)),
        (TVector<std::pair<TString, TString>>{{"EqualNulls", "0"}}));

    UNIT_ASSERT_VALUES_EQUAL(
        SettingPairs(BuildBlockHashJoinSettings(join, EJoinAlgoType::ReverseBlockJoin, 1, ctx)),
        (TVector<std::pair<TString, TString>>{{"BuildSide", "Left"}, {"EqualNulls", "0"}}));
}

Y_UNIT_TEST(OptimizerCreatesPhyBlockHashJoinWithEqualNulls) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    const auto join = MakeDqJoin(ctx, {}, {{"EqualNulls", "0"}, {"EqualNulls", "1"}});
    const auto settings = BuildBlockHashJoinSettings(join, EJoinAlgoType::GraceJoin, 2, ctx);

    const auto phyJoin = Build<TDqPhyBlockHashJoin>(ctx, pos)
        .LeftInput(join.LeftInput())
        .RightInput(join.RightInput())
        .LeftLabel(join.LeftLabel())
        .RightLabel(join.RightLabel())
        .JoinType(join.JoinType())
        .JoinKeys(join.JoinKeys())
        .LeftJoinKeyNames(join.LeftJoinKeyNames())
        .RightJoinKeyNames(join.RightJoinKeyNames())
        .Settings()
            .Add(settings)
            .Build()
        .Done();

    UNIT_ASSERT(TDqPhyBlockHashJoin::Match(phyJoin.Raw()));
    UNIT_ASSERT_VALUES_EQUAL(SettingPairs(settings), (TVector<std::pair<TString, TString>>{
        {"EqualNulls", "0"},
        {"EqualNulls", "1"},
    }));

    const auto ast = NCommon::ExprToPrettyString(ctx, phyJoin.Ref());
    UNIT_ASSERT_C(ast.Contains("DqPhyBlockHashJoin"), ast);
    UNIT_ASSERT_C(ast.Contains("EqualNulls"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" '"0")"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" '"1")"), ast);
}

} // DqOptEqualNulls
