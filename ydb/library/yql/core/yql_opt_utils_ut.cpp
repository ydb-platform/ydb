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

} // TYqlOptUtils


} // namespace NYql

