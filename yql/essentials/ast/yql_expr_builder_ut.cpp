#include "yql_expr.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TExprBuilder) {
    Y_UNIT_TEST(TestEmpty) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestRootAtom) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle()).Atom("ABC").Build();
        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Content(), "ABC");
    }

    Y_UNIT_TEST(TestRootAtomTwice) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Atom("ABC")
            .Atom("ABC")
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestRootEmptyList) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle()).List().Seal().Build();
        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 0);
    }

    Y_UNIT_TEST(TestRootEmptyListTwice) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List()
            .List()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestListWithAtoms) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .List()
                .Atom(0, "ABC")
                .Atom(1, "XYZ")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Content(), "ABC");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "XYZ");
    }

    Y_UNIT_TEST(TestMismatchChildIndex) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List()
            .Atom(1, "")
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestListWithAdd) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .List()
                .Add(0, ctx.Builder(TPositionHandle()).Atom("ABC").Build())
                .Atom(1, "XYZ")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Content(), "ABC");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "XYZ");
    }

    Y_UNIT_TEST(TestNestedListWithAtoms) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .List()
                .List(0)
                    .Atom(0, "ABC")
                    .Atom(1, "DEF")
                .Seal()
                .Atom(1, "XYZ")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "ABC");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "DEF");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "XYZ");
    }

    Y_UNIT_TEST(TestWrongLevelBuild) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongLevelSeal) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Seal(), yexception);
    }

    Y_UNIT_TEST(TestCallableWithAtoms) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Callable("Func")
                .Atom(0, "ABC")
                .Atom(1, "XYZ")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Content(), "Func");
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Content(), "ABC");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "XYZ");
    }

    Y_UNIT_TEST(TestNestedCallableWithAtoms) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Callable("Func1")
                .Callable(0, "Func2")
                    .Atom(0, "ABC")
                    .Atom(1, "DEF")
                .Seal()
                .Atom(1, "XYZ")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Content(), "Func1");
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Content(), "Func2");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "ABC");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "DEF");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "XYZ");
    }

    Y_UNIT_TEST(TestRootWorld) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle()).World().Build();
        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::World);
    }

    Y_UNIT_TEST(TestCallableWithWorld) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Callable("Func")
                .World(0)
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Content(), "Func");
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::World);
    }

    Y_UNIT_TEST(TestIncompleteRootLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIncompleteInnerLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List()
            .Lambda()
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestRootLambda) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle()).Lambda().Atom("ABC").Seal().Build();
        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "ABC");
    }

    Y_UNIT_TEST(TestRootLambdaWithBodyAsSet) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Set(ctx.Builder(TPositionHandle()).Atom("ABC").Build())
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "ABC");
    }

    Y_UNIT_TEST(TestInnerLambdaWithParam) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .List()
                .Lambda(0)
                    .Param("x")
                    .Atom("ABC")
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Head().Content(), "x");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "ABC");
    }

    Y_UNIT_TEST(TestDuplicateLambdaParamNames) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Param("x")
                .Atom("ABC")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestParamAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Param("aaa")
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestParamInList) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List()
                .Param("aaa")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestParamInCallable) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Callable("Func")
                .Param("aaa")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestParamAfterLambdaBody) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("aaa")
                .Atom("ABC")
                .Param("bbb")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIndexedAtomAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Atom(0, "ABC")
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIndexedListAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .List(0)
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIndexedWorldAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .World(0)
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIndexedCallableAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Callable(0, "Func")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestIndexedLambdaAtRoot) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda(0)
            .Atom("ABC")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongIndexAtomAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .Atom(1, "ABC")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongIndexListAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .List(1)
            .Seal()
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongIndexWorldAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .World(1)
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongIndexCallableAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .Callable(1, "Func")
            .Seal()
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongIndexLambdaAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .Lambda(1)
            .Atom("ABC")
            .Seal()
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestAddAtLambda) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle())
            .Lambda()
            .Add(1, ctx.Builder(TPositionHandle()).Atom("ABC").Build())
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestLambdaWithArgAsBody) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Param("y")
                .Arg("x")
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "x");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "y");
        UNIT_ASSERT_EQUAL(res->Child(1), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestIndexedArgAsLambdaBody) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle()).Lambda()
            .Param("x")
            .Arg(1, "x")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestWrongArgName) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle()).Lambda()
            .Param("x")
            .Arg("y")
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestLambdaWithArgInCallables) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
            .Param("x")
            .Param("y")
            .Callable("+")
                .Arg(0, "y")
                .Arg(1, "x")
            .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "x");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "y");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "+");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->ChildrenSize(), 2);
        UNIT_ASSERT_EQUAL(res->Child(1)->Child(0), res->Head().Child(1));
        UNIT_ASSERT_EQUAL(res->Child(1)->Child(1), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestNestedScopeInLambda) {
        TExprContext ctx;
        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Param("y")
                .Callable("Apply")
                    .Lambda(0)
                        .Param("x")
                        .Callable("+")
                            .Arg(0, "x")
                            .Arg(1, "y")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "x");
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Child(1)->Content(), "y");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Content(), "Apply");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Head().Head().Content(), "x");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Child(1)->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Child(1)->Content(), "+");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Child(1)->ChildrenSize(), 2);
        // nested x
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Child(1)->Child(0),
            res->Child(1)->Head().Head().Child(0));
        // outer y
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Head().Child(1)->Child(1),
            res->Head().Child(1));
    }

    Y_UNIT_TEST(TestNonIndexedArg) {
        TExprContext ctx;
        UNIT_ASSERT_EXCEPTION(ctx.Builder(TPositionHandle()).Lambda()
            .Param("x")
            .Callable("f")
                .Arg("x")
            .Seal()
            .Seal()
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestApplyLambdaArgAsRoot) {
        TExprContext ctx;
        auto lambda = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Arg("x")
            .Seal()
            .Build();

        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("y")
                .Apply(lambda).With(0, "y").Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "y");
        UNIT_ASSERT_EQUAL(res->Child(1), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestApplyLambdaArgInContainer) {
        TExprContext ctx;
        auto lambda = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Arg("x")
            .Seal()
            .Build();

        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
            .Param("y")
                .List()
                    .Apply(0, lambda).With(0, "y").Seal()
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "y");
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(res->Child(1)->ChildrenSize(), 1);
        UNIT_ASSERT_EQUAL(res->Child(1)->Child(0), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestApplyPartialLambdaArgAsRoot) {
        TExprContext ctx;
        auto lambda = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Callable("Func1")
                    .Callable(0, "Func2")
                        .Atom(0, "ABC")
                        .Arg(1, "x")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("y")
                .ApplyPartial(lambda->HeadPtr(), lambda->Child(1)->HeadPtr()).With(0, "y").Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "y");
        UNIT_ASSERT_EQUAL(res->Child(1)->Child(1), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestApplyPartialLambdaArgInContainer) {
        TExprContext ctx;
        auto lambda = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Callable("Func1")
                    .Callable(0, "Func2")
                        .Atom(0, "ABC")
                        .Arg(1, "x")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto res = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("y")
                .Callable("Func3")
                    .ApplyPartial(0, lambda->HeadPtr(), lambda->Child(1)->HeadPtr()).With(0, "y").Seal()
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res->Head().Head().Content(), "y");
        UNIT_ASSERT_EQUAL(res->Child(1)->Head().Child(1), res->Head().Child(0));
    }

    Y_UNIT_TEST(TestApplyOuterArg) {
        TExprContext ctx;
        auto ast = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("x")
                .Callable("Func1")
                    .Atom(0, "p1")
                    .Lambda(1)
                        .Callable("Func2")
                            .Atom(0, "ABC")
                            .Arg(1, "x")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto res1 = ctx.Builder(TPositionHandle())
            .Lambda()
                .Param("y")
                .Callable("Func3")
                    .ApplyPartial(0, nullptr, ast->Child(1)->Child(1)->ChildPtr(1))
                    .WithNode(*ast->Head().Child(0), "y").Seal()
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res1->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res1->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res1->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res1->Head().ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res1->Head().Head().Type(), TExprNode::Argument);
        UNIT_ASSERT_VALUES_EQUAL(res1->Head().Head().Content(), "y");
        UNIT_ASSERT_EQUAL(res1->Child(1)->Head().Child(1), res1->Head().Child(0));

        auto atom = ctx.Builder(TPositionHandle())
            .Atom("const")
            .Build();

        auto res2 = ctx.Builder(TPositionHandle())
            .Lambda()
                .Callable("Func3")
                    .ApplyPartial(0, nullptr, ast->Child(1)->Child(1)->ChildPtr(1))
                    .WithNode(ast->Head().Head(), TExprNode::TPtr(atom)).Seal()
                .Seal()
            .Seal()
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(res2->Type(), TExprNode::Lambda);
        UNIT_ASSERT_VALUES_EQUAL(res2->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res2->Head().Type(), TExprNode::Arguments);
        UNIT_ASSERT_VALUES_EQUAL(res2->Head().ChildrenSize(), 0);
        UNIT_ASSERT_EQUAL(res2->Child(1)->Head().ChildPtr(1), atom);
    }
}

} // namespace NYql
