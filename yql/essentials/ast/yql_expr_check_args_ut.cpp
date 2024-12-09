#include "yql_expr.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TExprCheckArguments) {
    Y_UNIT_TEST(TestDuplicateArgument) {
        TExprContext ctx;
        auto pos = TPositionHandle();
        auto arg0 = ctx.NewArgument(pos, "arg0");
        auto args = ctx.NewArguments(pos, { arg0 });
        auto body = ctx.Builder(pos)
            .Callable("+")
                .Add(0, arg0)
                .Add(1, arg0)
            .Seal()
            .Build();

        auto left = ctx.NewLambda(pos, std::move(args), std::move(body));

        auto arg1 = ctx.NewArgument(pos, "arg0");
        args = ctx.NewArguments(pos, { arg0, arg1 });
        body = ctx.Builder(pos)
            .Callable("+")
                .Add(0, arg0)
                .Add(1, arg1)
            .Seal()
            .Build();

        auto right = ctx.NewLambda(pos, std::move(args), std::move(body));

        auto root = ctx.Builder(pos)
            .Callable("SomeTopLevelCallableWithTwoLambdas")
                .Add(0, left)
                .Add(1, right)
            .Seal()
            .Build();

        UNIT_ASSERT_EXCEPTION_CONTAINS(CheckArguments(*root), yexception, "argument is duplicated, #[1]");
    }

    Y_UNIT_TEST(TestUnresolved) {
        TExprContext ctx;
        auto pos = TPositionHandle();

        auto arg1 = ctx.NewArgument(pos, "arg1");
        auto arg0 = ctx.NewArgument(pos, "arg0");

        auto innerLambdaBody = ctx.Builder(pos)
            .Callable("+")
                .Add(0, arg0)
                .Add(1, arg1)
            .Seal()
            .Build();

        auto innerLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { arg1 }), std::move(innerLambdaBody));

        auto outerLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { arg0 }), TExprNode::TPtr(innerLambda));

        auto root = ctx.Builder(pos)
            .Callable("SomeTopLevelCallableWithTwoLambdasAndFreeArg")
                .Add(0, outerLambda)
                .Add(1, innerLambda)
            .Seal()
            .Build();

        UNIT_ASSERT_EXCEPTION_CONTAINS(CheckArguments(*root), yexception, "detected unresolved arguments at top level: #[2]");

        root = ctx.Builder(pos)
            .Callable("SomeTopLevelCallableWithTwoLambdasAndFreeArg")
                .Add(0, outerLambda)
                .Add(1, innerLambda)
                .Add(2, ctx.NewArgument(pos, "arg3"))
            .Seal()
            .Build();

        UNIT_ASSERT_EXCEPTION_CONTAINS(CheckArguments(*root), yexception, "detected unresolved arguments at top level: #[2, 10]");
    }

    Y_UNIT_TEST(TestUnresolvedFreeArg) {
        TExprContext ctx;
        auto pos = TPositionHandle();
        auto arg = ctx.NewArgument(pos, "arg");
        UNIT_ASSERT_EXCEPTION_CONTAINS(CheckArguments(*arg), yexception, "detected unresolved arguments at top level: #[1]");
    }

    Y_UNIT_TEST(TestOk) {
        TExprContext ctx;
        auto pos = TPositionHandle();

        auto root = ctx.Builder(pos)
            .Callable("TopLevelCallableWithTwoLambdas")
                .Lambda(0)
                    .Param("one")
                    .Lambda()
                        .Param("two")
                        .Callable("+")
                            .Arg(0, "one")
                            .Arg(1, "two")
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Param("three")
                    .Callable("Not")
                        .Arg(0, "three")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        UNIT_ASSERT_NO_EXCEPTION(CheckArguments(*root));
    }
}

} // namespace NYql
