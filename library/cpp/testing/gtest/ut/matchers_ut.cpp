#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <array>
#include <vector>

constexpr std::string_view TEST_DATA_TEXT{"123\n"};
constexpr std::array<char, 4> TEST_DATA_ARRAY{'1', '2', '3', '\n'};
const std::vector<char> TEST_DATA_CHAR_VECTOR{'1', '2', '3', '\n'};

TEST(GoldenFileEq, Example) {
    EXPECT_THAT(TEST_DATA_TEXT, NGTest::GoldenFileEq(std::string(SRC_("golden/data.txt"))));
    EXPECT_THAT(TEST_DATA_ARRAY, NGTest::GoldenFileEq(TString(SRC_("golden/data.txt"))));
    EXPECT_THAT(TEST_DATA_CHAR_VECTOR, NGTest::GoldenFileEq(SRC_("golden/data.txt")));
}
