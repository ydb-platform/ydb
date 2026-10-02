#include <yt/yt/client/query_client/query_builder.h>
#include <yt/yt/client/query_client/table_hint.h>

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

TEST(TQueryBuilderTest, SourceHint)
{
    TQueryBuilder builder;

    TTableHint hint;
    hint.RequireSyncReplica = false;
    hint.PushDownGroupBy = true;
    builder.SetSource("fooTable", "foo");
    builder.SetSourceHint(hint);
    builder.AddSelectExpression("[id]");
    builder.AddWhereConjunct("[id] = 42");

    EXPECT_EQ(
        builder.Build(),
        "([id]) FROM [fooTable] AS foo WITH HINT \"{push_down_group_by=%true;require_sync_replica=%false;}\" WHERE [id] = 42");
}

TEST(TQueryBuilderTest, DefaultSourceHintOmitted)
{
    TQueryBuilder builder;

    builder.SetSource("fooTable");
    builder.SetSourceHint(TTableHint());
    builder.AddSelectExpression("[id]");

    EXPECT_EQ(builder.Build(), "([id]) FROM [fooTable]");
}

TEST(TQueryBuilderTest, SourceHintWithSubqueryThrows)
{
    TQueryBuilder builder;

    builder.SetSource("SELECT * FROM [fooTable]", /*syntaxVersion*/ 1, /*subquerySource*/ true);
    builder.SetSourceHint(TTableHint());
    builder.AddSelectExpression("[id]");

    EXPECT_THROW_WITH_SUBSTRING(builder.Build(), "Hint cannot be specified for a subquery source");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
