#include <ydb/library/yql/core/yql_opt_utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TYqlOptUtils) {

Y_UNIT_TEST(HasOnlyJoinType) {
    TExprContext ctx;
    auto node1 = ctx.Builder(TPositionHandle())
            .Atom("Atom")
        .Build();

    UNIT_ASSERT(HasOnlyOneJoinType(*node1, "Cross"));

    auto node2 = ctx.Builder(TPositionHandle())
            .List()
                .Atom(0, "Cross")
                .List(1, {})
                    .Atom(0, "Cross")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
                .List(2, {})
                    .Atom(0, "Cross")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
            .Seal()
        .Build();

    UNIT_ASSERT(HasOnlyOneJoinType(*node2, "Cross"));
    UNIT_ASSERT(!HasOnlyOneJoinType(*node2, "Inner"));

    auto node3 = ctx.Builder(TPositionHandle())
            .List()
                .Atom(0, "Cross")
                .List(1, {})
                    .Atom(0, "Cross")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
                .List(2, {})
                    .Atom(0, "Inner")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
            .Seal()
        .Build();

    UNIT_ASSERT(!HasOnlyOneJoinType(*node3, "Cross"));
    UNIT_ASSERT(!HasOnlyOneJoinType(*node3, "Inner"));

    auto node4 = ctx.Builder(TPositionHandle())
            .List()
                .Atom(0, "Inner")
                .List(1, {})
                    .Atom(0, "Inner")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
                .List(2, {})
                    .Atom(0, "Inner")
                    .Atom(1, "Atom")
                    .Atom(2, "Atom")
                .Seal()
            .Seal()
        .Build();


    UNIT_ASSERT(HasOnlyOneJoinType(*node4, "Inner"));
    UNIT_ASSERT(!HasOnlyOneJoinType(*node4, "Cross"));
}

Y_UNIT_TEST(GenNoClashColumnsBasic) {
    TExprContext ctx;
    TVector<const TItemExprType*> items;
    items.push_back(ctx.MakeType<TItemExprType>("a", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("b", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefix0", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefix1", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefix3", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefix4", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefixFoo", ctx.MakeType<TNullExprType>()));
    items.push_back(ctx.MakeType<TItemExprType>("_yql_prefix6", ctx.MakeType<TNullExprType>()));
    auto structType = ctx.MakeType<TStructExprType>(items);

    auto result = GenNoClashColumns(*structType, "_yql_prefix", 3);
    UNIT_ASSERT_EQUAL(result.size(), 3);
    UNIT_ASSERT_EQUAL(result[0], "_yql_prefix2");
    UNIT_ASSERT_EQUAL(result[1], "_yql_prefix5");
    UNIT_ASSERT_EQUAL(result[2], "_yql_prefix7");
}

Y_UNIT_TEST(GenNoClashColumnsEmpty) {
    TExprContext ctx;
    auto structType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
    auto result = GenNoClashColumns(*structType, "_yql_prefix", 3);
    UNIT_ASSERT_EQUAL(result.size(), 3);
    UNIT_ASSERT_EQUAL(result[0], "_yql_prefix0");
    UNIT_ASSERT_EQUAL(result[1], "_yql_prefix1");
    UNIT_ASSERT_EQUAL(result[2], "_yql_prefix2");
}

Y_UNIT_TEST(GenNoClashColumnsThrowsOnWrongPrefix) {
    TExprContext ctx;
    auto structType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
    UNIT_ASSERT_EXCEPTION(GenNoClashColumns(*structType, "prefix", 3), TYqlPanic);
}

} // TYqlOptUtils


} // namespace NYql

