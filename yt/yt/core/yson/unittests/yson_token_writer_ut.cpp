#include <yt/yt/core/test_framework/fixed_growth_string_output.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/token_writer.h>
#include <yt/yt/core/yson/writer.h>

namespace NYT::NYson {
namespace {

////////////////////////////////////////////////////////////////////////////////

void IntListTest(EYsonFormat format, size_t stringBufferSize)
{
    TString out1, out2;
    std::vector<i64> ints = {1LL << 62, 12345678, 1, -12345678, 12, -(1LL << 62), 12345678910121LL, -12345678910121LL, 1, -1, 2, -2, 0};
    std::vector<ui64> uints = {1ULL << 63, 1, 10, 100, 1000000000000, 0, 1ULL << 31};

    {
        TFixedGrowthStringOutput outStream(&out1, stringBufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginList();
        for (i64 val : ints) {
            if (format == EYsonFormat::Binary) {
                writer.WriteBinaryInt64(val);
            } else {
                writer.WriteTextInt64(val);
            }
            writer.WriteItemSeparator();
        }
        for (ui64 val : uints) {
            if (format == EYsonFormat::Binary) {
                writer.WriteBinaryUint64(val);
            } else {
                writer.WriteTextUint64(val);
            }
            writer.WriteItemSeparator();
        }
        writer.WriteEndList();

        writer.Finish();
    }

    {
        TStringOutput outStream(out2);
        TYsonWriter writer(&outStream, format);

        writer.OnBeginList();
        for (i64 val : ints) {
            writer.OnListItem();
            writer.OnInt64Scalar(val);
        }
        for (ui64 val : uints) {
            writer.OnListItem();
            writer.OnUint64Scalar(val);
        }
        writer.OnEndList();

        writer.Flush();
    }

    EXPECT_EQ(out1, out2);
}

TEST(TYsonTokenWriterTest, BinaryIntList)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        IntListTest(EYsonFormat::Binary, bufferSize);
    }
}

TEST(TYsonTokenWriterTest, TextIntList)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        IntListTest(EYsonFormat::Text, bufferSize);
    }
}

TEST(TYsonTokenWriterTest, BinaryString)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);
        writer.WriteBinaryString("Hello, world!");
        writer.Finish();

        EXPECT_EQ(out, "\1\x1AHello, world!");
    }
}

TEST(TYsonTokenWriterTest, TextString)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);
        writer.WriteTextString("Hello, world!");
        writer.Finish();

        EXPECT_EQ(out, "\"Hello, world!\"");
    }
}

TEST(TYsonTokenWriterTest, DifferentTypesBinaryMap)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginAttributes();
        writer.WriteBinaryString("type");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryString("map");
        writer.WriteItemSeparator();
        writer.WriteEndAttributes();

        writer.WriteBeginMap();
        writer.WriteBinaryString("double");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryDouble(2.71828);
        writer.WriteItemSeparator();
        writer.WriteBinaryString("boolean");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryBoolean(true);
        writer.WriteItemSeparator();
        writer.WriteBinaryString("entity");
        writer.WriteKeyValueSeparator();
        writer.WriteEntity();
        writer.WriteItemSeparator();
        writer.WriteEndMap();

        writer.Finish();

        EXPECT_EQ(out, "<\1\x08type=\1\6map;>{\1\014double=\3\x90\xF7\xAA\x95\t\xBF\5@;\1\016boolean=\5;\1\014entity=#;}");
    }
}

TEST(TYsonTokenWriterTest, DifferentTypesTextMap)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginAttributes();
        writer.WriteTextString("type");
        writer.WriteKeyValueSeparator();
        writer.WriteTextString("map");
        writer.WriteItemSeparator();
        writer.WriteEndAttributes();

        writer.WriteBeginMap();
        writer.WriteTextString("double");
        writer.WriteKeyValueSeparator();
        writer.WriteTextDouble(2.71828);
        writer.WriteItemSeparator();
        writer.WriteTextString("boolean");
        writer.WriteKeyValueSeparator();
        writer.WriteTextBoolean(true);
        writer.WriteItemSeparator();
        writer.WriteTextString("entity");
        writer.WriteKeyValueSeparator();
        writer.WriteEntity();
        writer.WriteItemSeparator();
        writer.WriteEndMap();

        writer.Finish();

        EXPECT_EQ(out, "<\"type\"=\"map\";>{\"double\"=2.71828;\"boolean\"=%true;\"entity\"=#;}");
    }
}

TEST(TYsonTokenWriterTest, WriteRawNodeUnchecked)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginList();
        writer.WriteRawNodeUnchecked("<a=b>{x=1;y=z}");
        writer.WriteItemSeparator();
        writer.WriteEndList();

        writer.Finish();

        EXPECT_EQ(out, "[<a=b>{x=1;y=z};]");
    }

    {
        TString out;
        TFixedGrowthStringOutput outStream(&out, /*bufferSize*/ 20);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginMap();
        EXPECT_THROW_THAT(writer.WriteRawNodeUnchecked("<a=b>{x=1;y=z}"), ::testing::HasSubstr("expected \"string\""));
    }
}

TEST(TYsonTokenWriterTest, ThroughZeroCopyOutputStreamWriter)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TFixedGrowthStringOutput outStream(&out, bufferSize);
        TZeroCopyOutputStreamWriter writer(&outStream);

        TStringBuf prefix = "Now some YSON: ";
        writer.Write(prefix.data(), prefix.size());

        TCheckedYsonTokenWriter tokenWriter(&writer);

        tokenWriter.WriteBeginAttributes();
        tokenWriter.WriteTextString("type");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteTextString("map");
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndAttributes();

        tokenWriter.WriteBeginMap();
        tokenWriter.WriteTextString("double");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteTextDouble(2.71828);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteTextString("boolean");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteTextBoolean(true);
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteTextString("entity");
        tokenWriter.WriteKeyValueSeparator();
        tokenWriter.WriteEntity();
        tokenWriter.WriteItemSeparator();
        tokenWriter.WriteEndMap();

        tokenWriter.Finish();

        TStringBuf suffix = " -- no more YSON";
        writer.Write(suffix.data(), suffix.size());
        writer.UndoRemaining();
        outStream.Finish();

        EXPECT_EQ(
            out,
            "Now some YSON: "
            "<\"type\"=\"map\";>{\"double\"=2.71828;\"boolean\"=%true;\"entity\"=#;}"
            " -- no more YSON");
    }
}

TEST(TYsonTokenWriterTest, TotalWrittenSize)
{
    TString out;
    TFixedGrowthStringOutput outStream(&out, 15);
    TZeroCopyOutputStreamWriter writer(&outStream);
    TUncheckedYsonTokenWriter tokenWriter(&writer);
    EXPECT_EQ(tokenWriter.GetTotalWrittenSize(), 0u);
    tokenWriter.WriteBinaryString("abcdef");
    EXPECT_EQ(tokenWriter.GetTotalWrittenSize(), 8u);
    tokenWriter.WriteBinaryString("abcd");
    EXPECT_EQ(tokenWriter.GetTotalWrittenSize(), 14u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
