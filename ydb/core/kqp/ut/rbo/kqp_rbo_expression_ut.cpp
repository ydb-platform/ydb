#include <ydb/core/kqp/opt/rbo/kqp_expression.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboExpression) {
    Y_UNIT_TEST(NullPropagationUsesOnlySpecifiedColumns) {
        TExprContext ctx;
        TPlanProps props;
        const TPositionHandle pos;
        const auto a = MakeColumnAccess(0, pos, &ctx, &props);
        const auto b = MakeColumnAccess(1, pos, &ctx, &props);
        const auto one = MakeConstant("Int32", "1", pos, &ctx);

        UNIT_ASSERT(a.IsNullWhen({0}));
        UNIT_ASSERT(!a.IsNullWhen({1}));
        UNIT_ASSERT(!a.IsNullWhen({}));
        UNIT_ASSERT(MakeBinaryPredicate("+", a, b).IsNullWhen({1}));
        UNIT_ASSERT(!MakeBinaryPredicate("Coalesce", a, b).IsNullWhen({0}));
        UNIT_ASSERT(MakeBinaryPredicate("Coalesce", a, b).IsNullWhen({0, 1}));
        UNIT_ASSERT(!MakeBinaryPredicate("Coalesce", a, one).IsNullWhen({0}));
    }

    Y_UNIT_TEST(NullRejectionDoesNotRequireNullPropagation) {
        TExprContext ctx;
        TPlanProps props;
        const TPositionHandle pos;
        const auto a = MakeColumnAccess(0, pos, &ctx, &props);
        const auto b = MakeColumnAccess(1, pos, &ctx, &props);
        const auto existsA = MakeUnaryCallable("Exists", a);
        const auto existsB = MakeUnaryCallable("Exists", b);

        UNIT_ASSERT(!existsA.IsNullWhen({0}));
        UNIT_ASSERT(existsA.RejectsNulls({0}));
        UNIT_ASSERT(!existsA.RejectsNulls({1}));
        UNIT_ASSERT(!MakeNegation(existsA).RejectsNulls({0}));
        const auto eitherExists = MakeBinaryPredicate("Or", existsA, existsB);
        UNIT_ASSERT(!eitherExists.RejectsNulls({0}));
        UNIT_ASSERT(eitherExists.RejectsNulls({0, 1}));

        // Neither an unknown callable nor null-safe equality supplies a proof.
        const auto unknown = MakeUnaryCallable("UnknownFunction", a);
        UNIT_ASSERT(!unknown.IsNullWhen({0}));
        UNIT_ASSERT(!unknown.RejectsNulls({0}));
        UNIT_ASSERT(!MakeBinaryPredicate("AggrEquals", a, b).RejectsNulls({0, 1}));
    }
}

} // namespace NKikimr::NKqp
