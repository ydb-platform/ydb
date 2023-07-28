#include <gtest/gtest.h>

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTagSet, Api)
{
    TTagSet s1{{TTag{"foo", "bar"}}};
    TTagSet s2{{TTag{"foo", "zog"}}};
}

TEST(TTagSet, Subsets)
{
    TTagIdList tags{1, 2, 3};

    auto listProjections = [&] (
        TTagIndexList parents,
        TTagIndexList children,
        TTagIndexList required,
        TTagIndexList excluded,
        TTagIndexList alternative)
    {
        std::vector<TTagIdList> subsets;
        RangeSubsets(tags, parents, children, required, excluded, alternative, [&subsets] (auto list) {
            subsets.push_back(list);
        });

        std::sort(subsets.begin(), subsets.end());
        return subsets;
    };

    TTagIndexList noParents = {NoTagSentinel, NoTagSentinel, NoTagSentinel};
    TTagIndexList noChildren = {NoTagSentinel, NoTagSentinel, NoTagSentinel};
    TTagIndexList noAlternatives = {NoTagSentinel, NoTagSentinel, NoTagSentinel};

    auto full = listProjections(noParents, noChildren, {}, {}, noAlternatives);
    ASSERT_EQ(static_cast<size_t>(8), full.size());

    {
        auto actual = listProjections({NoTagSentinel, 0, 1}, noChildren, {}, {}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections({NoTagSentinel, 0, 0}, noChildren, {}, {}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
            { 1, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, noChildren, {0}, {}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
            { 1, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, noChildren, {0, 1}, {}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { 1, 2 },
            { 1, 2, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, noChildren, {}, {2}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 2 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, noChildren, {}, {}, {NoTagSentinel, NoTagSentinel, 1});
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 1, 3 },
            { 2 },
            { 3 }
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, {NoTagSentinel, 0, 0}, {}, {}, noAlternatives);
        std::vector<TTagIdList> expected = {
            { },
            { 1, 2, 3 },
            { 2 },
            { 2, 3 },
            { 3 }
        };
        ASSERT_EQ(expected, actual);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
