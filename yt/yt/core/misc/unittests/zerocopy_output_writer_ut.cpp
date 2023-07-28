#include <yt/yt/core/test_framework/fixed_growth_string_output.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/zerocopy_output_writer.h>

#include <util/random/fast.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TZeroCopyOutputStreamWriter, TestBasic)
{
    constexpr int GrowthSize = 7;
    TString string;
    auto buffer1 = TString("abcdef");
    auto buffer2 = TString("kinda long buffer");

    TFixedGrowthStringOutput stream(&string, GrowthSize);
    {
        TZeroCopyOutputStreamWriter writer(&stream);

        EXPECT_EQ(static_cast<ssize_t>(writer.RemainingBytes()), 0);
        EXPECT_EQ(writer.GetTotalWrittenSize(), 0u);

        writer.Write(buffer1.data(), buffer1.size());
        EXPECT_EQ(writer.RemainingBytes(), 7u);
        EXPECT_EQ(writer.GetTotalWrittenSize(), buffer1.length());

        writer.UndoRemaining();
        EXPECT_EQ(writer.RemainingBytes(), 0u);
        EXPECT_EQ(writer.GetTotalWrittenSize(), buffer1.length());

        writer.Write(buffer2.data(), buffer2.length());
        EXPECT_EQ(writer.GetTotalWrittenSize(), buffer1.length() + buffer2.length());
        EXPECT_EQ(static_cast<ssize_t>(writer.RemainingBytes()), GrowthSize);
    }

    stream.Finish();
    EXPECT_EQ(string, buffer1 + buffer2);
}

TEST(TZeroCopyOutputStreamWriter, TestStress)
{
    TString string;
    constexpr auto StringSize = 1000;
    TFastRng64 gen(42);
    for (int i = 0; i < StringSize; ++i) {
        string.push_back('a' + gen.Uniform(0, 26));
    }

    for (int growthSize = 1; growthSize < 20; ++growthSize) {
        auto writtenBytes = 0;
        TString outputString;
        TFixedGrowthStringOutput stream(&outputString, growthSize);
        TZeroCopyOutputStreamWriter writer(&stream);
        while (true) {
            auto toWrite = gen.Uniform(1, 3 * growthSize);
            toWrite = Min(toWrite, string.size() - writtenBytes);
            writer.Write(string.data() + writtenBytes, toWrite);
            writtenBytes += toWrite;

            if (writtenBytes == std::ssize(string)) {
                break;
            }

            if (writer.RemainingBytes() > 0) {
                *writer.Current() = string[writtenBytes];
                ++writtenBytes;
                writer.Advance(1);
            }
        }
        writer.UndoRemaining();
        stream.Finish();
        EXPECT_EQ(string, outputString) << "test for growth size " << growthSize << " failed";
    }
}

TEST(TZeroCopyOutputStreamWriter, TestVarInt)
{
    constexpr auto GrowthSize = 4;
    TString outputString;
    TFixedGrowthStringOutput stream(&outputString, GrowthSize);
    TZeroCopyOutputStreamWriter writer(&stream);

    EXPECT_EQ(1, WriteVarInt(&writer, static_cast<i32>(-17)));
    EXPECT_EQ(4u, writer.RemainingBytes());
    EXPECT_EQ(1, WriteVarInt32(&writer, static_cast<i32>(-18)));
    EXPECT_EQ(3u, writer.RemainingBytes());
    EXPECT_EQ(5, WriteVarInt(&writer, std::numeric_limits<i32>::min()));
    EXPECT_EQ(4u, writer.RemainingBytes());
    EXPECT_EQ(5, WriteVarInt32(&writer, std::numeric_limits<i32>::min()));
    EXPECT_EQ(4u, writer.RemainingBytes());

    EXPECT_EQ(1, WriteVarInt(&writer, static_cast<ui32>(10)));
    EXPECT_EQ(3u, writer.RemainingBytes());
    EXPECT_EQ(1, WriteVarUint32(&writer, static_cast<ui32>(11)));
    EXPECT_EQ(2u, writer.RemainingBytes());
    EXPECT_EQ(5, WriteVarInt(&writer, std::numeric_limits<ui32>::max()));
    EXPECT_EQ(4u, writer.RemainingBytes());
    EXPECT_EQ(5, WriteVarUint32(&writer, std::numeric_limits<ui32>::max()));
    EXPECT_EQ(4u, writer.RemainingBytes());

    EXPECT_EQ(1, WriteVarInt(&writer, static_cast<i64>(-23)));
    EXPECT_EQ(3u, writer.RemainingBytes());
    EXPECT_EQ(1, WriteVarInt64(&writer, static_cast<i64>(-22)));
    EXPECT_EQ(2u, writer.RemainingBytes());
    EXPECT_EQ(10, WriteVarInt(&writer, std::numeric_limits<i64>::min()));
    EXPECT_EQ(4u, writer.RemainingBytes());
    EXPECT_EQ(10, WriteVarInt64(&writer, std::numeric_limits<i64>::min()));
    EXPECT_EQ(4u, writer.RemainingBytes());

    EXPECT_EQ(2, WriteVarInt(&writer, static_cast<ui64>(200)));
    EXPECT_EQ(2u, writer.RemainingBytes());
    EXPECT_EQ(2, WriteVarUint64(&writer, static_cast<ui64>(211)));
    EXPECT_EQ(0u, writer.RemainingBytes());
    EXPECT_EQ(10, WriteVarInt(&writer, std::numeric_limits<ui64>::max()));
    EXPECT_EQ(4u, writer.RemainingBytes());
    EXPECT_EQ(10, WriteVarUint64(&writer, std::numeric_limits<ui64>::max()));
    EXPECT_EQ(4u, writer.RemainingBytes());

    writer.UndoRemaining();
    stream.Finish();

    EXPECT_EQ(
        outputString,
        "\x21\x23"
        "\xFF\xFF\xFF\xFF\x0F"
        "\xFF\xFF\xFF\xFF\x0F"
        "\x0A\x0B"
        "\xFF\xFF\xFF\xFF\x0F"
        "\xFF\xFF\xFF\xFF\x0F"
        "\x2D\x2B"
        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"
        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"
        "\xC8\x01" "\xD3\x01"
        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"
        "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
