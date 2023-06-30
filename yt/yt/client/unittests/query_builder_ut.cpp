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

    b.SetLimit(43);

    EXPECT_EQ(xIndex, 0);
    EXPECT_EQ(yIndex, 1);
    EXPECT_EQ(zIndex, 2);

    EXPECT_EQ(b.Build(),
        "(x), (y) AS y_alias, (z) "
        "FROM [//t] "
        "WHERE (x > y_alias) AND (y = 177 OR y % 2 = 0) "
        "ORDER BY (z) ASC, (x) DESC, (x + y) DESC, (z - y_alias) "
        "GROUP BY (x + y * z) AS group_expr, (x - 1) "
        "LIMIT 43");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
