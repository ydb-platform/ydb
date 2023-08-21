#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/uuid_text.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

void TestBidirectionalTextYqlUuidConversion(TStringBuf bytes, TStringBuf text)
{
    std::array<char, UuidYqlTextSize> bytesToText;
    TextYqlUuidFromBytes(bytes, bytesToText.data());
    EXPECT_EQ(TString(bytesToText.data(), bytesToText.size()), text);

    std::array<char, UuidBinarySize> textToBytes;
    TextYqlUuidToBytes(text, textToBytes.data());
    EXPECT_EQ(TString(textToBytes.data(), textToBytes.size()), bytes);
}

TEST(TUuidConverterTest, TextYql)
{
    TestBidirectionalTextYqlUuidConversion(
        TStringBuf("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", UuidBinarySize),
        "00000000-0000-0000-0000-000000000000");

    TestBidirectionalTextYqlUuidConversion(
        "\x01\x10\x20\x30\x40\x50\x60\x70\x80\x90\xa0\xb0\xc0\xd0\xe0\xf0",
        "30201001-5040-7060-8090-a0b0c0d0e0f0");
}

TEST(TUuidConverterTest, InvalidTextYql)
{
    std::array<char, UuidBinarySize> textToBytes;
    EXPECT_THROW_WITH_SUBSTRING(
        TextYqlUuidToBytes("00000000-0000-0000-000000-0000000000", textToBytes.data()),
        "Unexpected character: actual \"0\", expected \"-\"");
    EXPECT_THROW_WITH_SUBSTRING(
        TextYqlUuidToBytes("00000000-0000-0000-0000-00-0000000000", textToBytes.data()),
        "Invalid text YQL UUID length");
    EXPECT_THROW_WITH_SUBSTRING(
        TextYqlUuidToBytes("g0000000-0000-0000-0000-000000000000", textToBytes.data()),
        "Could not parse hex byte");
}

TEST(TUuidConverterTest, Guid)
{
    TString bytes = "\x01\x10\x20\x30\x40\x50\x60\x70\x80\x90\xa0\xb0\xc0\xd0\xe0\xf0";
    auto guid = GuidFromBytes(bytes);
    std::array<char, UuidBinarySize> guidToBytes;
    GuidToBytes(guid, guidToBytes.data());
    EXPECT_EQ(
        bytes,
        TStringBuf(guidToBytes.data(), guidToBytes.size()));


    EXPECT_EQ(guid.Parts32[0], 0xc0d0e0f0U);
    EXPECT_EQ(guid.Parts32[1], 0x8090a0b0U);
    EXPECT_EQ(guid.Parts32[2], 0x40506070U);
    EXPECT_EQ(guid.Parts32[3], 0x01102030U);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NComplexTypes
