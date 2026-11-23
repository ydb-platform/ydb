#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/embedding.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/vector.h>

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace NYdb {

TEST(Embedding, Float32) {
    const TVector<float> values = {1.0f, -2.0f, 0.5f};
    const auto value = NValueHelpers::Embedding(values);
    TValueParser parser(value);

    EXPECT_EQ(parser.GetPrimitiveType(), EPrimitiveType::Bytes);
    EXPECT_EQ(parser.GetBytes(), std::string("\x00\x00\x80\x3f\x00\x00\x00\xc0\x00\x00\x00\x3f\x01", 13));
}

TEST(Embedding, IntegersAreConvertedToFloat32) {
    const std::array<std::int64_t, 2> values = {-2, 16777217};
    const auto value = NValueHelpers::Embedding(values);
    TValueParser parser(value);

    EXPECT_EQ(parser.GetBytes(), std::string("\x00\x00\x00\xc0\x00\x00\x80\x4b\x01", 9));
}

TEST(Embedding, UnsignedIntegersAreConvertedToFloat32) {
    const std::array<std::uint16_t, 1> values = {2};
    const auto value = NValueHelpers::Embedding(values);
    TValueParser parser(value);

    EXPECT_EQ(parser.GetBytes(), std::string("\x00\x00\x00\x40\x01", 5));
}

TEST(Embedding, Float64IsConvertedToFloat32) {
    const std::vector<double> values = {1.00000001};
    const auto value = NValueHelpers::Embedding(values);
    TValueParser parser(value);

    EXPECT_EQ(parser.GetBytes(), std::string("\x00\x00\x80\x3f\x01", 5));
}

TEST(Embedding, EmptyHasFormatByte) {
    const auto value = NValueHelpers::Embedding(std::vector<float>{});
    TValueParser parser(value);

    EXPECT_EQ(parser.GetPrimitiveType(), EPrimitiveType::Bytes);
    EXPECT_EQ(parser.GetBytes(), std::string("\x01", 1));
}

} // namespace NYdb
