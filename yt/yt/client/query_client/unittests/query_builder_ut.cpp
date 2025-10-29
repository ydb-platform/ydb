#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/core/test_framework/framework.h>

#include <gtest/gtest.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryBuilderTest, Build)
{
    TQueryBuilder builder;

    builder.SetLimit(10);
    builder.SetOffset(15);
    builder.SetSource("fooTable");

    builder.AddWhereConjunct("[id] = 42");
    builder.AddWhereConjunct("[some_other_field] < 15");

    builder.AddSelectExpression("[some_field] * [some_other_field]", "res");
    builder.AddOrderByExpression("[res]", EOrderByDirection::Descending);
    builder.AddOrderByExpression("[some_field]", EOrderByDirection::Ascending);

    builder.AddJoinExpression("barTable", "bar", "fooTable.[id] = barTable.[id]", ETableJoinType::Left, "id = 0");

    auto source = builder.Build();

    EXPECT_NE(source.find("FROM [fooTable]"), source.npos);
    EXPECT_NE(source.find("LIMIT 10"), source.npos);
    EXPECT_NE(source.find("OFFSET 15"), source.npos);
    EXPECT_NE(source.find("ORDER BY ([res]) DESC, ([some_field]) ASC"), source.npos);
    EXPECT_NE(source.find("WHERE ([id] = 42) AND ([some_other_field] < 15)"), source.npos);
    EXPECT_NE(source.find("([some_field] * [some_other_field]) AS res"), source.npos);
    EXPECT_NE(source.find("LEFT JOIN [barTable] AS [bar] ON fooTable.[id] = barTable.[id] AND id = 0"), source.npos);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
