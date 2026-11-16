#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log_rules.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_rules.h>

#include <yql/essentials/core/yql_expr_csee.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

struct TLimitFixture {
    TExprContext Ctx;
    const TPositionHandle Pos = Ctx.AppendPosition({});

    TCoUint64 Literal(TStringBuf value) {
        return Build<TCoUint64>(Ctx, Pos).Literal().Build(value).Done();
    }

    TCoParameter Parameter() {
        return Build<TCoParameter>(Ctx, Pos)
            .Name().Build("count")
            .Type<TCoDataType>().Type().Build("Uint64").Build()
            .Done();
    }

    TExprBase Random() {
        return TExprBase(Ctx.NewCallable(Pos, "RandomNumber", {Parameter().Ptr()}));
    }

    TCoAggrAdd Sum(const TExprBase& left, const TExprBase& right) {
        return Build<TCoAggrAdd>(Ctx, Pos).Left(left).Right(right).Done();
    }

    TCoAsList Values() {
        const auto field = Build<TCoNameValueTuple>(Ctx, Pos).Name().Build("key").Value(Literal("1")).Done();
        const auto row = Build<TCoAsStruct>(Ctx, Pos).Add(field).Done();
        return Build<TCoAsList>(Ctx, Pos).Add(row).Done();
    }

    TCoLambda KeySelector() {
        return Build<TCoLambda>(Ctx, Pos)
            .Args({"row"})
            .Body<TCoMember>().Struct("row").Name().Build("key").Build()
            .Done();
    }

    TCoSort Sort(const TExprBase& input) {
        return Build<TCoSort>(Ctx, Pos)
            .Input(input)
            .KeySelectorLambda(KeySelector())
            .SortDirections<TCoBool>().Literal().Build("true").Build()
            .Done();
    }

    TCoExtractMembers Extract(const TExprBase& input) {
        return Build<TCoExtractMembers>(Ctx, Pos)
            .Input(input)
            .Members(NDq::BuildAtomList("key", Pos, Ctx))
            .Done();
    }

    TExprBase Count(TStringBuf callable, const TExprBase& input, const TExprBase& count) {
        return TExprBase(Ctx.NewCallable(Pos, callable, {input.Ptr(), count.Ptr()}));
    }

    TCoTopBase Top(TStringBuf callable, const TExprBase& input, const TExprBase& count) {
        return Build<TCoTopBase>(Ctx, Pos)
            .CallableName(callable)
            .Input(input)
            .Count(count)
            .KeySelectorLambda(KeySelector())
            .SortDirections<TCoBool>().Literal().Build("true").Build()
            .Done();
    }

    TDqStage Producer() {
        return Build<TDqStage>(Ctx, Pos)
            .Inputs().Build()
            .Program().Args({}).Body<TCoToStream>().Input(Values()).Build().Build()
            .Settings().Build()
            .Done();
    }

    TDqCnUnionAll Union(const TDqStage& producer) {
        return Build<TDqCnUnionAll>(Ctx, Pos)
            .Output().Stage(producer).Index().Build("0").Build()
            .Done();
    }

    TDqPhyPrecompute Precompute(const TDqStage& producer) {
        return Build<TDqPhyPrecompute>(Ctx, Pos)
            .Connection<TDqCnValue>()
                .Output().Stage(producer).Index().Build("0").Build()
                .Build()
            .Done();
    }

    TParentsMap Parents(const TExprBase& root) {
        TExprNode::TPtr scopedRoot;
        UNIT_ASSERT(UpdateCompletness(root.Ptr(), scopedRoot, Ctx) == IGraphTransformer::TStatus::Ok);
        TParentsMap parents;
        GatherParents(*scopedRoot, parents);
        return parents;
    }

    TDqStage AssertFinalStage(const TExprBase& output, const TDqCnUnionAll& input) {
        const auto stage = output.Cast<TDqCnUnionAll>().Output().Stage().Cast<TDqStage>();
        UNIT_ASSERT_VALUES_EQUAL(stage.Inputs().Size(), 1);
        UNIT_ASSERT(stage.Inputs().Item(0).Raw() == input.Raw());
        UNIT_ASSERT_VALUES_EQUAL(stage.Program().Args().Size(), 1);
        return stage;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpLimitCounts) {
    Y_UNIT_TEST(SaturatingLiteralSum) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        struct TCase {
            const char* Take;
            const char* Skip;
            const char* Expected;
        };
        for (const auto& test : {
            TCase{"0", "0", "0"},
            TCase{"1", "2", "3"},
            TCase{"0", "18446744073709551615", "18446744073709551615"},
            TCase{"18446744073709551614", "1", "18446744073709551615"},
            TCase{"18446744073709551615", "1", "18446744073709551615"},
            TCase{"1", "18446744073709551615", "18446744073709551615"}})
        {
            const auto take = Build<TCoUint64>(ctx, pos).Literal().Build(test.Take).Done();
            const auto skip = Build<TCoUint64>(ctx, pos).Literal().Build(test.Skip).Done();
            const auto count = BuildKqpTakeSkipCount(take, skip, ctx);
            UNIT_ASSERT_VALUES_EQUAL(count.Cast<TCoUint64>().Literal().Value(), test.Expected);
        }
    }

    Y_UNIT_TEST(VolatileCountsRequireMaterializationBeforeCopying) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto one = Build<TCoUint64>(ctx, pos).Literal().Build("1").Done();
        const auto parameter = Build<TCoParameter>(ctx, pos)
            .Name().Build("count")
            .Type<TCoDataType>().Type().Build("Uint64").Build()
            .Done();
        const auto sum = Build<TCoAggrAdd>(ctx, pos).Left(one).Right(parameter).Done();
        UNIT_ASSERT(IsKqpRepeatableExpr(one));
        UNIT_ASSERT(IsKqpRepeatableExpr(parameter));
        UNIT_ASSERT(IsKqpRepeatableExpr(sum));

        const auto random = TExprBase(ctx.NewCallable(pos, "RandomNumber", {parameter.Ptr()}));
        const auto volatileSum = Build<TCoAggrAdd>(ctx, pos).Left(one).Right(random).Done();
        UNIT_ASSERT(!IsKqpRepeatableExpr(random));
        UNIT_ASSERT(!IsKqpRepeatableExpr(volatileSum));
        for (const TStringBuf callable : {"Udf", "ScriptUdf", "SqlCall"}) {
            UNIT_ASSERT(!IsKqpRepeatableExpr(TExprBase(ctx.NewCallable(pos, callable, {}))));
        }

        const auto precompute = Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage<TDqStage>()
                        .Inputs().Build()
                        .Program()
                            .Args({})
                            .Body<TCoToStream>()
                                .Input<TCoAsList>().Add(random).Build()
                                .Build()
                            .Build()
                        .Settings().Build()
                        .Build()
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();
        UNIT_ASSERT(IsKqpRepeatableExpr(precompute));
        UNIT_ASSERT(IsKqpRepeatableExpr(Build<TCoAggrAdd>(ctx, pos).Left(one).Right(precompute).Done()));
    }

    Y_UNIT_TEST(TakeSkipSortUsesSaturatingCountWithExtractMembers) {
        struct TCase {
            const char* Take;
            const char* Skip;
            const char* Expected;
        };
        for (bool extract : {false, true}) {
            for (const auto& test : {
                TCase{"2", "1", "3"},
                TCase{"18446744073709551615", "2", "18446744073709551615"}})
            {
                TLimitFixture f;
                const auto sort = f.Sort(f.Values());
                const auto projected = f.Extract(sort);
                const auto takeCount = f.Literal(test.Take);
                const auto skipCount = f.Literal(test.Skip);
                const auto skip = f.Count("Skip", extract ? TExprBase(projected) : TExprBase(sort), skipCount);
                const auto take = f.Count("Take", skip, takeCount);
                const auto parents = f.Parents(take);
                auto output = KqpRewriteTakeSortToTopSort(take, f.Ctx, parents);

                UNIT_ASSERT(output.Raw() != take.Raw());
                if (extract) {
                    const auto members = output.Cast<TCoExtractMembers>();
                    UNIT_ASSERT(members.Members().Raw() == projected.Members().Raw());
                    output = members.Input();
                }
                const auto finalTake = output.Cast<TCoTake>();
                const auto finalSkip = finalTake.Input().Cast<TCoSkip>();
                const auto top = finalSkip.Input().Cast<TCoTopSort>();
                UNIT_ASSERT(finalTake.Count().Raw() == takeCount.Raw());
                UNIT_ASSERT(finalSkip.Count().Raw() == skipCount.Raw());
                UNIT_ASSERT(top.Input().Raw() == sort.Input().Raw());
                UNIT_ASSERT(top.KeySelectorLambda().Raw() == sort.KeySelectorLambda().Raw());
                UNIT_ASSERT(top.SortDirections().Raw() == sort.SortDirections().Raw());
                UNIT_ASSERT_VALUES_EQUAL(top.Count().Cast<TCoUint64>().Literal().Value(), test.Expected);
            }
        }
    }

    Y_UNIT_TEST(SharedVolatileTakeSkipSortIsNotRewritten) {
        for (bool extract : {false, true}) {
            TLimitFixture f;
            const auto sort = f.Sort(f.Values());
            const auto random = f.Random();
            const auto takeCount = f.Sum(f.Literal("1"), random);
            const auto input = extract ? TExprBase(f.Extract(sort)) : TExprBase(sort);
            const auto take = f.Count("Take", f.Count("Skip", input, random), takeCount);
            const auto parents = f.Parents(take);
            UNIT_ASSERT(KqpRewriteTakeSortToTopSort(take, f.Ctx, parents).Raw() == take.Raw());
        }
    }

    Y_UNIT_TEST(TakeSkipSortRetainsSharedConsumers) {
        enum class EShared { Sort, Extract, Skip };
        for (auto sharing : {EShared::Sort, EShared::Extract, EShared::Skip}) {
            TLimitFixture f;
            const auto sort = f.Sort(f.Values());
            const auto extract = f.Extract(sort);
            const auto skip = f.Count("Skip", extract, f.Literal("1"));
            const auto take = f.Count("Take", skip, f.Literal("2"));
            const auto shared = sharing == EShared::Sort ? TExprBase(sort)
                : sharing == EShared::Extract ? TExprBase(extract) : skip;
            const auto other = f.Ctx.NewCallable(f.Pos, "Length", {shared.Ptr()});
            const auto root = TExprBase(f.Ctx.NewList(f.Pos, {take.Ptr(), other}));
            const auto parents = f.Parents(root);
            UNIT_ASSERT(KqpRewriteTakeSortToTopSort(take, f.Ctx, parents).Raw() == take.Raw());
        }
    }

    Y_UNIT_TEST(VolatileCountsUseOneFinalStage) {
        for (const TStringBuf callable : {"Take", "Limit", "Top", "TopSort"}) {
            TLimitFixture f;
            const auto input = f.Union(f.Producer());
            const auto random = f.Random();
            const bool top = callable == "Top" || callable == "TopSort";
            const auto node = top ? TExprBase(f.Top(callable, input, random)) : f.Count(callable, input, random);
            const auto parents = f.Parents(node);
            const auto output = KqpBuildFinalLimitStage(node, f.Ctx, parents, false);
            const auto stage = f.AssertFinalStage(output, input);
            const auto body = stage.Program().Body();

            UNIT_ASSERT_VALUES_EQUAL(body.Ref().Content(), callable);
            UNIT_ASSERT(body.Cast<TCoInputBase>().Input().Raw() == stage.Program().Args().Arg(0).Raw());
            if (top) {
                const auto finalTop = body.Cast<TCoTopBase>();
                UNIT_ASSERT(finalTop.Count().Raw() == random.Raw());
                UNIT_ASSERT(finalTop.KeySelectorLambda().Raw() == node.Cast<TCoTopBase>().KeySelectorLambda().Raw());
                UNIT_ASSERT(finalTop.SortDirections().Raw() == node.Cast<TCoTopBase>().SortDirections().Raw());
            } else {
                UNIT_ASSERT(body.Cast<TCoTakeBase>().Count().Raw() == random.Raw());
            }
        }
    }

    Y_UNIT_TEST(SharedVolatileTakeSkipCountsStayTogether) {
        for (const TStringBuf callable : {"Take", "Limit"}) {
            TLimitFixture f;
            const auto input = f.Union(f.Producer());
            const auto random = f.Random();
            const auto takeCount = f.Sum(f.Literal("1"), random);
            const auto node = f.Count(callable, f.Count("Skip", input, random), takeCount);
            const auto parents = f.Parents(node);
            const auto output = KqpBuildFinalLimitStage(node, f.Ctx, parents, false);
            const auto stage = f.AssertFinalStage(output, input);
            const auto take = stage.Program().Body().Cast<TCoTakeBase>();
            const auto skip = take.Input().Cast<TCoSkip>();

            UNIT_ASSERT_VALUES_EQUAL(take.Ref().Content(), callable);
            UNIT_ASSERT(take.Count().Raw() == takeCount.Raw());
            UNIT_ASSERT(skip.Count().Raw() == random.Raw());
            UNIT_ASSERT(take.Count().Cast<TCoAggrAdd>().Right().Raw() == skip.Count().Raw());
            UNIT_ASSERT(skip.Input().Raw() == stage.Program().Args().Arg(0).Raw());
        }
    }

    Y_UNIT_TEST(RepeatableCountsKeepExistingStageOptimization) {
        for (const TStringBuf callable : {"Take", "Limit", "Top", "TopSort"}) {
            TLimitFixture f;
            const auto input = f.Union(f.Producer());
            const bool top = callable == "Top" || callable == "TopSort";
            for (const auto& count : {TExprBase(f.Literal("2")), TExprBase(f.Parameter())}) {
                const auto node = top ? TExprBase(f.Top(callable, input, count)) : f.Count(callable, input, count);
                const auto parents = f.Parents(node);
                UNIT_ASSERT(KqpBuildFinalLimitStage(node, f.Ctx, parents, false).Raw() == node.Raw());
                if (!top) {
                    const auto takeSkip = f.Count(callable, f.Count("Skip", input, count), count);
                    const auto skipParents = f.Parents(takeSkip);
                    UNIT_ASSERT(KqpBuildFinalLimitStage(takeSkip, f.Ctx, skipParents, false).Raw() == takeSkip.Raw());
                }
            }
        }
    }

    Y_UNIT_TEST(FinalStageRetainsConnectionAndOutputConsumerGuards) {
        enum class EShared { Connection, Output, Producer };
        for (auto sharing : {EShared::Connection, EShared::Output, EShared::Producer}) {
            TLimitFixture f;
            const auto producer = f.Producer();
            const auto input = f.Union(producer);
            const auto node = f.Count("Take", input, f.Random());
            auto otherInput = input;
            if (sharing == EShared::Output) {
                otherInput = Build<TDqCnUnionAll>(f.Ctx, f.Pos).Output(input.Output()).Done();
            } else if (sharing == EShared::Producer) {
                otherInput = f.Union(producer);
            }
            const auto other = f.Count("Take", otherInput, f.Literal("1"));
            const auto root = TExprBase(f.Ctx.NewList(f.Pos, {node.Ptr(), other.Ptr()}));
            const auto parents = f.Parents(root);

            UNIT_ASSERT(KqpBuildFinalLimitStage(node, f.Ctx, parents, false).Raw() == node.Raw());
            const auto output = KqpBuildFinalLimitStage(node, f.Ctx, parents, true);
            if (sharing == EShared::Producer) {
                f.AssertFinalStage(output, input);
            } else {
                UNIT_ASSERT(output.Raw() == node.Raw());
            }
        }
    }

    Y_UNIT_TEST(FinalStageRejectsCountsAndKeysDependingOnItsProducer) {
        enum class EDependency { Take, Skip, Top, Key };
        for (auto dependency : {EDependency::Take, EDependency::Skip, EDependency::Top, EDependency::Key}) {
            for (bool sameProducer : {false, true}) {
                TLimitFixture f;
                const auto producer = f.Producer();
                const auto input = f.Union(producer);
                const auto precompute = f.Precompute(sameProducer ? producer : f.Producer());
                const auto value = Build<TCoMember>(f.Ctx, f.Pos).Struct(precompute).Name().Build("key").Done();
                const auto random = f.Random();
                const auto count = f.Sum(random, value);
                TExprBase node = f.Count("Take", input, count);
                if (dependency == EDependency::Skip) {
                    node = f.Count("Take", f.Count("Skip", input, count), f.Literal("1"));
                } else if (dependency == EDependency::Top) {
                    node = f.Top("TopSort", input, count);
                } else if (dependency == EDependency::Key) {
                    const auto row = Build<TCoArgument>(f.Ctx, f.Pos).Name("row").Done();
                    const auto field = Build<TCoMember>(f.Ctx, f.Pos).Struct(row).Name().Build("key").Done();
                    const auto key = Build<TCoLambda>(f.Ctx, f.Pos).Args({row}).Body(f.Sum(field, value)).Done();
                    const auto top = f.Top("TopSort", input, random);
                    node = TExprBase(f.Ctx.ChangeChild(top.Ref(), TCoTopBase::idx_KeySelectorLambda, key.Ptr()));
                }
                const auto parents = f.Parents(node);
                const auto output = KqpBuildFinalLimitStage(node, f.Ctx, parents, true);
                if (sameProducer) {
                    UNIT_ASSERT(output.Raw() == node.Raw());
                } else {
                    f.AssertFinalStage(output, input);
                }
            }
        }
    }
}

} // namespace NKikimr::NKqp::NOpt
