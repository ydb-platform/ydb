#include <yt/yt/core/ypath/trie.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NYPath {
namespace {

///////////////////////////////////////////////////////////////////////////////

struct TTrieTest
    : public testing::TestWithParam<bool>
{ };

TEST_P(TTrieTest, EmptyTrie)
{
    TTrie trie(/*paths*/ {}, /*enableFlattening*/ GetParam());
    EXPECT_EQ(trie.GetType(), ETrieNodeType::Outlier);
    EXPECT_EQ(trie.Visit("a").GetType(), ETrieNodeType::Outlier);
}

TEST_P(TTrieTest, EmptyPath)
{
    TTrie trie({""}, /*enableFlattening*/ GetParam());
    EXPECT_EQ(trie.GetType(), ETrieNodeType::Leaf);
}

TEST_P(TTrieTest, Simple)
{
    TTrie trie({"/a/b"}, /*enableFlattening*/ GetParam());
    EXPECT_EQ(trie.GetType(), ETrieNodeType::Intermediary);
    EXPECT_EQ(trie.Visit("c").GetType(), ETrieNodeType::Outlier);
    auto view = trie.Visit("a");
    EXPECT_EQ(view.GetType(), ETrieNodeType::Intermediary);
    EXPECT_EQ(view.Visit("c").GetType(), ETrieNodeType::Outlier);
    view = view.Visit("b");
    EXPECT_EQ(view.GetType(), ETrieNodeType::Leaf);
    EXPECT_EQ(view.Visit("c").GetType(), ETrieNodeType::AfterLeaf);
}

TEST_P(TTrieTest, Copyable)
{
    TTrie trie({""}, /*enableFlattening*/ GetParam());
    EXPECT_EQ(trie.GetType(), ETrieNodeType::Leaf);

    auto trieA = trie;
    EXPECT_EQ(trieA.GetType(), ETrieNodeType::Leaf);

    TTrie trieB;
    EXPECT_EQ(trieB.GetType(), ETrieNodeType::Outlier);
    trieB = trie;
    EXPECT_EQ(trieB.GetType(), ETrieNodeType::Leaf);
}

TEST_P(TTrieTest, Moveable)
{
    TTrie trie({"/a"}, /*enableFlattening*/ GetParam());
    auto movedTrie = std::move(trie);
    EXPECT_EQ(movedTrie.GetType(), ETrieNodeType::Intermediary);
    EXPECT_EQ(movedTrie.Visit("c").GetType(), ETrieNodeType::Outlier);
    EXPECT_EQ(movedTrie.Visit("a").GetType(), ETrieNodeType::Leaf);
    EXPECT_EQ(movedTrie.Visit("a").Visit("b").GetType(), ETrieNodeType::AfterLeaf);
}

TEST_P(TTrieTest, Merge)
{
    TTrie trie({"/a"}, /*enableFlattening*/ GetParam());
    TTrie otherTrie({"/b"}, /*enableFlattening*/ GetParam());
    trie.Merge(otherTrie);
    auto triePaths = trie.GetNestedPaths();
    ASSERT_EQ(std::ssize(triePaths), 2);
    EXPECT_EQ(triePaths[0], "/a");
    EXPECT_EQ(triePaths[1], "/b");
}

TEST_P(TTrieTest, BranchedTree)
{
    //    |
    //    a
    //   / \
    //  a   b
    //     / \
    //    a   b
    //    |
    //    a
    TTrie trie({"/a/b/a/a", "/a/a", "/a/b/b"}, /*enableFlattening*/ GetParam());
    auto view = trie.Visit("a");
    EXPECT_EQ(view.GetType(), ETrieNodeType::Intermediary);

    // Left branch (/a/a)
    {
        auto branchView = view.Visit("a");
        EXPECT_EQ(branchView.GetType(), ETrieNodeType::Leaf);
        EXPECT_EQ(branchView.Visit("c").GetType(), ETrieNodeType::AfterLeaf);
    }

    // Right branch (/a/b)
    {
        auto branchView = view.Visit("b");
        EXPECT_EQ(branchView.GetType(), ETrieNodeType::Intermediary);
        EXPECT_EQ(branchView.Visit("c").GetType(), ETrieNodeType::Outlier);

        // Left branch (/a/b/a)
        {
            auto view = branchView.Visit("a");
            EXPECT_EQ(view.GetType(), ETrieNodeType::Intermediary);
            view = view.Visit("a");
            EXPECT_EQ(view.GetType(), ETrieNodeType::Leaf);
            EXPECT_EQ(view.Visit("c").GetType(), ETrieNodeType::AfterLeaf);
        }

        // Right branch (/a/b/b)
        {
            auto view = branchView.Visit("b");
            EXPECT_EQ(view.GetType(), ETrieNodeType::Leaf);
            EXPECT_EQ(view.Visit("c").GetType(), ETrieNodeType::AfterLeaf);
        }
    }
}

TEST_P(TTrieTest, ExtendPathPolicy)
{
    TTrie trie({"/a"}, /*enableFlattening*/ GetParam(), ETrieNestedPathPolicy::Extend);
    EXPECT_EQ(trie.Visit("a").GetType(), ETrieNodeType::Leaf);
    trie.AddPath("/a/a");
    EXPECT_EQ(trie.Visit("a").GetType(), ETrieNodeType::Intermediary);
    EXPECT_EQ(trie.Visit("a").Visit("a").GetType(), ETrieNodeType::Leaf);
}

TEST_P(TTrieTest, ShortenPathPolicy)
{
    TTrie trie({"/a/a"}, /*enableFlattening*/ GetParam(), ETrieNestedPathPolicy::Shorten);
    EXPECT_EQ(trie.Visit("a").Visit("a").GetType(), ETrieNodeType::Leaf);
    trie.AddPath("/a");
    EXPECT_EQ(trie.Visit("a").GetType(), ETrieNodeType::Leaf);

    TTrie branchedTrie({"/a/a", "/a/b/c"}, /*enableFlattening*/ GetParam(), ETrieNestedPathPolicy::Shorten);
    EXPECT_EQ(branchedTrie.Visit("a").Visit("a").GetType(), ETrieNodeType::Leaf);
    EXPECT_EQ(branchedTrie.Visit("a").Visit("b").Visit("c").GetType(), ETrieNodeType::Leaf);
    branchedTrie.AddPath("/a/b");
    EXPECT_EQ(branchedTrie.Visit("a").Visit("b").GetType(), ETrieNodeType::Leaf);
}

TEST_P(TTrieTest, FlatTraversalFrame)
{
    TTrie trie({"/a"}, /*enableFlattening*/ GetParam());
    auto rootFrame = trie.Traverse();
    ASSERT_EQ(std::ssize(rootFrame.GetUnvisitedTokens()), 1);
    EXPECT_EQ(rootFrame.GetUnvisitedTokens()[0], "a");

    auto aView = rootFrame.Visit("a");
    EXPECT_EQ(aView.GetType(), ETrieNodeType::Leaf);
    EXPECT_EQ(std::ssize(rootFrame.GetUnvisitedTokens()), 0);
}

TEST_P(TTrieTest, BranchedTraversalFrame)
{
    TTrie trie({"/a", "/b"}, /*enableFlattening*/ GetParam());
    auto rootFrame = trie.Traverse();
    ASSERT_EQ(std::ssize(rootFrame.GetUnvisitedTokens()), 2);
    EXPECT_EQ(rootFrame.GetUnvisitedTokens()[0], "a");
    EXPECT_EQ(rootFrame.GetUnvisitedTokens()[1], "b");

    auto aView = rootFrame.Visit("a");
    EXPECT_EQ(aView.GetType(), ETrieNodeType::Leaf);
    ASSERT_EQ(std::ssize(rootFrame.GetUnvisitedTokens()), 1);
    EXPECT_EQ(rootFrame.GetUnvisitedTokens()[0], "b");

    auto bView = rootFrame.Visit("b");
    EXPECT_EQ(bView.GetType(), ETrieNodeType::Leaf);
    EXPECT_EQ(std::ssize(rootFrame.GetUnvisitedTokens()), 0);
}

TEST_P(TTrieTest, GetNestedPaths)
{
    std::vector<TYPath> paths = {"/a/a", "/a/b/c"};
    TTrie trie(paths, /*enableFlattening*/ GetParam());
    EXPECT_EQ(trie.GetNestedPaths(), paths);
}

INSTANTIATE_TEST_SUITE_P(
    TTrieTest,
    TTrieTest,
    /*setViaYson*/ testing::Values(true, false),
    /*evalGenerateName*/ [] (const testing::TestParamInfo<TTrieTest::ParamType>& setViaYson) {
        return setViaYson.param ? "WithFlattening" : "WithoutFlattening";
    });

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYPath
