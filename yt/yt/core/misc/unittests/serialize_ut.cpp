#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/containers/non_empty.h>

#include <string>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializeNonEmptyTest, SaveAndLoadOptionalWithValue)
{
    TStringStream stream;
    TStreamSaveContext saveContext(&stream);
    TStreamLoadContext loadContext(&stream);

    std::optional<TNonEmpty<std::vector<int>>> first({1, 2, 3});
    Save(saveContext, first);
    saveContext.Finish();

    std::optional<TNonEmpty<std::vector<int>>> second;
    Load(loadContext, second);

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->size(), 3u);
    EXPECT_EQ(second->Get()[0], 1);
    EXPECT_EQ(second->Get()[1], 2);
    EXPECT_EQ(second->Get()[2], 3);
}

TEST(TSerializeNonEmptyTest, SaveAndLoadOptionalEmpty)
{
    TStringStream stream;
    TStreamSaveContext saveContext(&stream);
    TStreamLoadContext loadContext(&stream);

    std::optional<TNonEmpty<std::vector<int>>> first;
    Save(saveContext, first);
    saveContext.Finish();

    std::optional<TNonEmpty<std::vector<int>>> second({99});
    Load(loadContext, second);

    EXPECT_FALSE(second.has_value());
}

TEST(TSerializeNonEmptyTest, SaveAndLoadOptionalWithStrings)
{
    TStringStream stream;
    TStreamSaveContext saveContext(&stream);
    TStreamLoadContext loadContext(&stream);

    std::optional<TNonEmpty<std::vector<std::string>>> first({"hello", "world"});
    Save(saveContext, first);
    saveContext.Finish();

    std::optional<TNonEmpty<std::vector<std::string>>> second;
    Load(loadContext, second);

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->size(), 2u);
    EXPECT_EQ(second->Get()[0], "hello");
    EXPECT_EQ(second->Get()[1], "world");
}

TEST(TSerializeNonEmptyTest, SaveAndLoadNestedOptionalNonEmpty)
{
    TStringStream stream;
    TStreamSaveContext saveContext(&stream);
    TStreamLoadContext loadContext(&stream);

    // Create nested: optional<TNonEmpty<vector<optional<TNonEmpty<vector<int>>>>>>
    using TInner = std::optional<TNonEmpty<std::vector<int>>>;
    using TOuter = std::optional<TNonEmpty<std::vector<TInner>>>;

    std::vector<TInner> innerVec;
    innerVec.push_back(TNonEmpty<std::vector<int>>({1, 2, 3}));
    innerVec.push_back(std::nullopt);
    innerVec.push_back(TNonEmpty<std::vector<int>>({4, 5}));

    TOuter first = TNonEmpty<std::vector<TInner>>(std::move(innerVec));
    TOuter second;

    Save(saveContext, first);
    saveContext.Finish();

    Load(loadContext, second);

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->size(), 3u);

    ASSERT_TRUE((*second)[0].has_value());
    EXPECT_EQ((*second)[0]->size(), 3u);
    EXPECT_EQ((*(*second)[0])[0], 1);
    EXPECT_EQ((*(*second)[0])[1], 2);
    EXPECT_EQ((*(*second)[0])[2], 3);

    EXPECT_FALSE((*second)[1].has_value());

    ASSERT_TRUE((*second)[2].has_value());
    EXPECT_EQ((*second)[2]->size(), 2u);
    EXPECT_EQ((*(*second)[2])[0], 4);
    EXPECT_EQ((*(*second)[2])[1], 5);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
