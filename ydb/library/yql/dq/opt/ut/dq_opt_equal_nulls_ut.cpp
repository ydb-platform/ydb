#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TCoNameValueTuple EqualNullsSetting(TExprContext& ctx, TPositionHandle pos, TStringBuf keyIndex) {
    return Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build("EqualNulls")
        .Value<TCoUint32>()
            .Literal().Build(keyIndex)
            .Build()
        .Done();
}

} // namespace

Y_UNIT_TEST_SUITE(DqOptEqualNulls) {

Y_UNIT_TEST(PhyScalarHashJoinAstUsesUint32) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    const auto dummy = Build<TCoVoid>(ctx, pos).Done();
    const auto dummyList = Build<TCoAtomList>(ctx, pos).Done();

    const auto phyJoin = Build<TDqPhyScalarHashJoin>(ctx, pos)
        .LeftInput(dummy)
        .RightInput(dummy)
        .LeftLabel(ctx.NewAtom(pos, "L"))
        .RightLabel(ctx.NewAtom(pos, "R"))
        .JoinType().Build("Inner")
        .JoinKeys(Build<TDqJoinKeyTupleList>(ctx, pos).Done())
        .LeftJoinKeyNames(dummyList)
        .RightJoinKeyNames(dummyList)
        .Settings()
            .Add(EqualNullsSetting(ctx, pos, "0"))
            .Add(EqualNullsSetting(ctx, pos, "1"))
            .Build()
        .Done();

    const auto ast = NCommon::ExprToPrettyString(ctx, phyJoin.Ref());
    UNIT_ASSERT_C(ast.Contains("DqPhyScalarHashJoin"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" (Uint32 '"0"))"), ast);
    UNIT_ASSERT_C(ast.Contains(R"("EqualNulls" (Uint32 '"1"))"), ast);
}

Y_UNIT_TEST(PhyScalarHashJoinAllowsMissingSettings) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    const auto dummy = Build<TCoVoid>(ctx, pos).Done();
    const auto dummyList = Build<TCoAtomList>(ctx, pos).Done();

    const auto phyJoin = Build<TDqPhyScalarHashJoin>(ctx, pos)
        .LeftInput(dummy)
        .RightInput(dummy)
        .LeftLabel(ctx.NewAtom(pos, "L"))
        .RightLabel(ctx.NewAtom(pos, "R"))
        .JoinType().Build("Inner")
        .JoinKeys(Build<TDqJoinKeyTupleList>(ctx, pos).Done())
        .LeftJoinKeyNames(dummyList)
        .RightJoinKeyNames(dummyList)
        .Done();

    UNIT_ASSERT(!phyJoin.Settings());
    UNIT_ASSERT_VALUES_EQUAL(phyJoin.Ref().ChildrenSize(), 8U);
}

} // DqOptEqualNulls
