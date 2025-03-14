#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/range_helpers.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRangeHelpersTest, ZipMutable)
{
    std::vector<int> vectorA(4);
    std::vector<int> vectorB = {1, 2, 3};
    for (auto [a, b] : ZipMutable(vectorA, vectorB)) {
        *a = *b + 1;
    }

    auto expectedA = std::vector<int>{2, 3, 4, 0};
    EXPECT_EQ(expectedA, vectorA);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRangeHelpersTest, RangeToVector)
{
    auto data = std::vector<std::string>{"A", "B", "C", "D"};
    auto range = std::ranges::views::transform(data, [] (std::string x) {
        return "_" + x;
    });

    std::initializer_list<std::string> expectedValues{"_A", "_B", "_C", "_D"};
    EXPECT_EQ(std::vector<std::string>(expectedValues), RangeTo<std::vector<std::string>>(range));
    using TSomeCompactVector = TCompactVector<std::string, 4>;
    EXPECT_EQ(TSomeCompactVector(expectedValues), RangeTo<TSomeCompactVector>(range));
}

TEST(TRangeHelpersTest, RangeToString)
{
    auto data = "_sample_"sv;
    auto range = std::ranges::views::filter(data, [] (char x) {
        return x != '_';
    });
    auto expectedData = "sample"sv;

    EXPECT_EQ(std::string(expectedData), RangeTo<std::string>(range));
    EXPECT_EQ(TString(expectedData), RangeTo<TString>(range));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
