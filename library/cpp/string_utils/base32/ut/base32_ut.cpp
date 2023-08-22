#include <library/cpp/string_utils/base32/base32.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/yexception.h>

#include <vector>

namespace {

    static const std::vector<std::string> TEST_DATA = {
        {},
        "\x00",
        "\x01\x02",
        "\x01\x02\x03",
        "\x03\x02\x01",
        "\x10\x20\x30\x40",
        "\x10\x20\x30\x40\x50",
        "\xFF\xFF\xFF",
        "\xFF\xFE\xFD\xFC\xFB\xFA",
    };

} // namespace

TEST(base32, encode)
{
    EXPECT_EQ(Base32Encode({}), "");
    EXPECT_EQ(Base32Encode({"\x00\x40", 2}), "ABAA====");
    EXPECT_EQ(Base32Encode("apple"), "MFYHA3DF");
    EXPECT_EQ(Base32Encode("TestTest"), "KRSXG5CUMVZXI===");
    EXPECT_EQ(Base32Encode("1234567890"), "GEZDGNBVGY3TQOJQ");
}

TEST(base32, decode_strict)
{
    EXPECT_EQ(Base32StrictDecode(""), "");
    EXPECT_EQ(Base32StrictDecode("MFYHA3DF"), "apple");

    EXPECT_EQ(Base32StrictDecode("KRSXG5CUMVZXI"), "TestTest");
    EXPECT_EQ(Base32StrictDecode("KRSXG5CUMVZXI==="), "TestTest");
    EXPECT_THROW(Base32StrictDecode("KRSXG5CUMVZXI=======A"), yexception);

    EXPECT_EQ(Base32StrictDecode("AA======"), std::string(1, '\x00'));
    EXPECT_THROW(Base32StrictDecode("AAA="), yexception);

    EXPECT_EQ(Base32StrictDecode("AE======"), "\x01");
    EXPECT_THROW(Base32StrictDecode("AB======"), yexception); // "\x00\x40"

    EXPECT_THROW(Base32StrictDecode("invalid"), yexception);
    EXPECT_THROW(Base32StrictDecode("\xFF\xFF"), yexception);
    EXPECT_THROW(Base32StrictDecode(std::string_view{"A\0", 2}), yexception);
}

TEST(base32, decode)
{
    EXPECT_EQ(Base32Decode(""), "");
    EXPECT_EQ(Base32Decode("MFYHA3DF"), "apple");

    EXPECT_EQ(Base32Decode("KRSXG5CUMVZXI"), "TestTest");
    EXPECT_EQ(Base32Decode("KRSXG5CUMVZXI==="), "TestTest");
    EXPECT_NO_THROW(Base32Decode("KRSXG5CUMVZXI=======A"));

    EXPECT_EQ(Base32Decode("AA======"), std::string(1, '\x00'));
    EXPECT_NO_THROW(Base32Decode("AAA="));

    EXPECT_EQ(Base32Decode("AE======"), "\x01");
    EXPECT_NO_THROW(Base32Decode("AB======")); // "\x00\x40"

    EXPECT_NO_THROW(Base32Decode("invalid"));
    EXPECT_NO_THROW(Base32Decode("\xFF\xFF"));
    EXPECT_NO_THROW(Base32Decode(std::string_view{"A\0", 2}));
}

TEST(base32, encode_decode)
{
    for (const auto& data : TEST_DATA) {
        EXPECT_THAT(Base32StrictDecode(Base32Encode(data)), ::testing::ContainerEq(data));
    }
}
