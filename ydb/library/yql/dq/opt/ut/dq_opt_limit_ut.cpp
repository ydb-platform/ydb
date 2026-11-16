#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

using namespace NNodes;

Y_UNIT_TEST_SUITE(DqLimitCounts) {
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
            const auto count = BuildDqTakeSkipCount(take, skip, ctx);
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
        UNIT_ASSERT(IsDqRepeatableExpr(one));
        UNIT_ASSERT(IsDqRepeatableExpr(parameter));
        UNIT_ASSERT(IsDqRepeatableExpr(sum));

        const auto random = TExprBase(ctx.NewCallable(pos, "RandomNumber", {parameter.Ptr()}));
        const auto volatileSum = Build<TCoAggrAdd>(ctx, pos).Left(one).Right(random).Done();
        UNIT_ASSERT(!IsDqRepeatableExpr(random));
        UNIT_ASSERT(!IsDqRepeatableExpr(volatileSum));
        for (const TStringBuf callable : {"Udf", "ScriptUdf", "SqlCall"}) {
            UNIT_ASSERT(!IsDqRepeatableExpr(TExprBase(ctx.NewCallable(pos, callable, {}))));
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
        UNIT_ASSERT(IsDqRepeatableExpr(precompute));
        UNIT_ASSERT(IsDqRepeatableExpr(Build<TCoAggrAdd>(ctx, pos).Left(one).Right(precompute).Done()));
    }
}

} // namespace NYql::NDq
