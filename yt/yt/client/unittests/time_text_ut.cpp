#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/time_text.h>

namespace NYT::NComplexTypes {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TimeConverter, Correct)
{
    EXPECT_EQ(BinaryTimeFromText("2021-09-03", ESimpleLogicalValueType::Date), 18873U);

    EXPECT_EQ(BinaryTimeFromText("2021-09-03T10:12:41Z", ESimpleLogicalValueType::Datetime), 1630663961U);

    EXPECT_EQ(BinaryTimeFromText("2021-09-03T10:12:41Z", ESimpleLogicalValueType::Timestamp), 1630663961000000ULL);
    EXPECT_EQ(BinaryTimeFromText("2021-09-03T10:12:41.1Z", ESimpleLogicalValueType::Timestamp), 1630663961100000ULL);
    EXPECT_EQ(BinaryTimeFromText("2021-09-03T10:12:41.123456Z", ESimpleLogicalValueType::Timestamp), 1630663961123456ULL);
}

TEST(TimeConverter, InvalidLength)
{
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09", ESimpleLogicalValueType::Date),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T", ESimpleLogicalValueType::Date),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T10:12:41Z", ESimpleLogicalValueType::Date),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T10:12:41.1Z", ESimpleLogicalValueType::Date),
        "Invalid date string length");

    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03", ESimpleLogicalValueType::Datetime),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T10:12:41", ESimpleLogicalValueType::Datetime),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T10:12:41.1Z", ESimpleLogicalValueType::Datetime),
        "Invalid date string length");

    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03", ESimpleLogicalValueType::Timestamp),
        "Invalid date string length");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T10:12:41.1234567Z", ESimpleLogicalValueType::Timestamp),
        "Invalid date string length");
}

TEST(TimeConverter, InvalidFormat)
{
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-3T", ESimpleLogicalValueType::Date),
        "Could not parse date");
    EXPECT_THROW_WITH_SUBSTRING(
        BinaryTimeFromText("2021-09-03T23:59:59H", ESimpleLogicalValueType::Datetime),
        "Could not parse date");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
