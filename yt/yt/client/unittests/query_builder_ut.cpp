#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/query_client/query_builder.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryBuilderTest, Simple)
{
    TQueryBuilder b;
    int xIndex = b.AddSelectExpression("x");
    int yIndex = b.AddSelectExpression("y", "y_alias");
    int zIndex = b.AddSelectExpression("z");

    b.SetSource("//t");

    b.AddWhereConjunct("x > y_alias");
    b.AddWhereConjunct("y = 177 OR y % 2 = 0");

    b.AddOrderByAscendingExpression("z");
    b.AddOrderByDescendingExpression("x");
    b.AddOrderByExpression("x + y", EOrderByDirection::Descending);
    b.AddOrderByExpression("z - y_alias");

    b.AddGroupByExpression("x + y * z", "group_expr");
    b.AddGroupByExpression("x - 1");

    b.AddHavingConjunct("group_expr > 42");
    b.AddHavingConjunct("group_expr < 420");

    b.AddJoinExpression("table1", "lookup1", "(idx) = (lookup1.idx)", ETableJoinType::Inner);
    b.AddJoinExpression("table2", "lookup2", "(idx) = (lookup2.idx)", ETableJoinType::Left);

    b.SetLimit(43);

    EXPECT_EQ(xIndex, 0);
    EXPECT_EQ(yIndex, 1);
    EXPECT_EQ(zIndex, 2);

    EXPECT_EQ(b.Build(),
        "(x), (y) AS y_alias, (z) "
        "FROM [//t] "
        "JOIN [table1] AS [lookup1] ON (idx) = (lookup1.idx) "
        "LEFT JOIN [table2] AS [lookup2] ON (idx) = (lookup2.idx) "
        "WHERE (x > y_alias) AND (y = 177 OR y % 2 = 0) "
        "GROUP BY (x + y * z) AS group_expr, (x - 1) "
        "HAVING (group_expr > 42) AND (group_expr < 420) "
        "ORDER BY (z) ASC, (x) DESC, (x + y) DESC, (z - y_alias) "
        "LIMIT 43");
}

TEST(TQueryBuilderTest, SourceAlias)
{
    TQueryBuilder b;
    b.AddSelectExpression("t_alias.x");
    b.SetSource("//t", "t_alias");

    EXPECT_EQ(b.Build(),
        "(t_alias.x) "
        "FROM [//t] AS t_alias");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
