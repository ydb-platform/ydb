#include "plan_utils.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

using namespace NYql;
using namespace NNodes;
using namespace NPlanUtils;

TExprNode::TPtr ConstantExprNode(TExprContext& ctx, i64 c) {
    auto pos = ctx.AppendPosition({});
    return ctx.Builder(pos).Callable("Int64").Atom(0, ToString(c)).Seal().Build();
}

TString PrettifyForPlan(TExprContext& ctx, const TExprBase& ex) {
    Cerr << "Prettify node:\n" << NCommon::ExprToPrettyString(ctx, *ex.Raw()) << Endl;
    TString result = PrettyExprStr(ex);
    Cerr << "Result: [" << result << "]" << Endl;
    return result;
}

Y_UNIT_TEST_SUITE(PlanUtilsTest) {
    Y_UNIT_TEST(Cmp) {
        TExprContext ctx;
        auto lambda = Build<TCoLambda>(ctx, ctx.AppendPosition({}))
            .Args({"arg"})
            .Body<TCoCmpGreater>()
                .Left<TCoMember>()
                    .Struct("arg")
                    .Name().Build("a")
                    .Build()
                .Right(ConstantExprNode(ctx, 42))
                .Build()
            .Done();
        UNIT_ASSERT_STRINGS_EQUAL(PrettifyForPlan(ctx, lambda), "arg.a > 42");
    }

    Y_UNIT_TEST(Exists) {
        TExprContext ctx;
        auto lambda = Build<TCoLambda>(ctx, ctx.AppendPosition({}))
            .Args({"s"})
            .Body<TCoExists>()
                .Optional<TCoMember>()
                    .Struct("s")
                    .Name().Build("m")
                    .Build()
                .Build()
            .Done();
        UNIT_ASSERT_STRINGS_EQUAL(PrettifyForPlan(ctx, lambda), "Exist(s.m)");
    }

    Y_UNIT_TEST(And) {
        TExprContext ctx;
        auto lambda = Build<TCoLambda>(ctx, ctx.AppendPosition({}))
            .Args({"row"})
            .Body<TCoAnd>()
                .Add<TCoNot>()
                    .Value<TCoExists>()
                        .Optional<TCoMember>()
                            .Struct("row")
                            .Name().Build("x")
                            .Build()
                        .Build()
                    .Build()
                .Add<TCoCmpEqual>()
                    .Left<TCoMember>()
                        .Struct("row")
                        .Name().Build("y")
                        .Build()
                    .Right<TCoMember>()
                        .Struct("row")
                        .Name().Build("z")
                        .Build()
                    .Build()
                .Build()
            .Done();
        UNIT_ASSERT_STRINGS_EQUAL(PrettifyForPlan(ctx, lambda), "NOT Exist(row.x) AND row.y == row.z");
    }
}
