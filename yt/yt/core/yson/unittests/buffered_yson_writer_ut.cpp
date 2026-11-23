#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/stream.h>
#include <yt/yt/core/yson/writer.h>

#include <util/string/escape.h>

#include <array>
#include <stdexcept>
#include <vector>

namespace NYT::NYson {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TBufferedYsonWriterTest
    : public ::testing::Test
{
protected:
    TStringStream Stream;
    StrictMock<TMockYsonConsumer> Mock;

    void Run(TBufferedYsonWriter* writer)
    {
        writer->Flush();
        ParseYson(TYsonInput(&Stream), &Mock);
    }
};

class TThrowingOutputStream final
    : public IOutputStream
{
public:
    const std::vector<size_t>& GetWriteLengths() const
    {
        return WriteLengths_;
    }

private:
    std::vector<size_t> WriteLengths_;

    void DoWrite(const void* /*data*/, size_t length) override
    {
        WriteLengths_.push_back(length);
        throw std::runtime_error("Write failed");
    }
};

class TSizeLimitedOutputStream final
    : public IOutputStream
{
public:
    explicit TSizeLimitedOutputStream(size_t limit)
        : Limit_(limit)
    { }

    size_t GetWrittenSize() const
    {
        return WrittenSize_;
    }

private:
    const size_t Limit_;

    size_t WrittenSize_ = 0;

    void DoWrite(const void* /*data*/, size_t length) override
    {
        if (length > Limit_ - WrittenSize_) {
            throw std::runtime_error("Output size limit exceeded");
        }
        WrittenSize_ += length;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define TEST_SCALAR(value, type) \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary); \
        writer.On##type##Scalar(value); \
        Run(&writer); \
    } \
        \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TBufferedYsonWriter writer(&Stream, EYsonFormat::Text); \
        writer.On##type##Scalar(value); \
        Run(&writer); \
    } \


TEST_F(TBufferedYsonWriterTest, String)
{
    std::string value = "YSON";
    TEST_SCALAR(TStringBuf(value), String)
}

TEST_F(TBufferedYsonWriterTest, EmptyString)
{
    TEST_SCALAR(TStringBuf(), String)
}

TEST_F(TBufferedYsonWriterTest, Int64)
{
    i64 value = 100500424242ll;
    TEST_SCALAR(value, Int64)
}

TEST_F(TBufferedYsonWriterTest, Uint64)
{
    ui64 value = 100500424242llu;
    TEST_SCALAR(value, Uint64)
}

TEST_F(TBufferedYsonWriterTest, Boolean)
{
    bool value = true;
    TEST_SCALAR(value, Boolean)
}

TEST_F(TBufferedYsonWriterTest, Double)
{
    double value = 1.7976931348623157e+308;
    TEST_SCALAR(value, Double)
}

TEST_F(TBufferedYsonWriterTest, Inf)
{
    double value = std::numeric_limits<double>::infinity();
    TEST_SCALAR(value, Double)
}

TEST_F(TBufferedYsonWriterTest, MinusInf)
{
    double value = -std::numeric_limits<double>::infinity();
    TEST_SCALAR(value, Double)
}

TEST_F(TBufferedYsonWriterTest, IntegralDouble)
{
    double value = 42.0;
    TEST_SCALAR(value, Double)
}

TEST_F(TBufferedYsonWriterTest, LongestTextDouble)
{
    double value = -1.2345678901234567e-10;
    TEST_SCALAR(value, Double)
}

TEST_F(TBufferedYsonWriterTest, NaN)
{
    TBufferedYsonWriter writer(&Stream, EYsonFormat::Text);
    writer.OnDoubleScalar(std::numeric_limits<double>::quiet_NaN());
    writer.Flush();
    EXPECT_EQ(Stream.Str(), "%nan");
}

TEST_F(TBufferedYsonWriterTest, BinaryScalarListCrossesFlushBoundaries)
{
    constexpr int RepeatCount = 4;

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginList());
    for (int index = 0; index < RepeatCount; ++index) {
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(std::numeric_limits<i64>::min()));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnUint64Scalar(std::numeric_limits<ui64>::max()));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(1.25));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnBooleanScalar(true));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnEntity());
    }
    EXPECT_CALL(Mock, OnEndList());

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary);
    writer.OnBeginList();
    for (int index = 0; index < RepeatCount; ++index) {
        writer.OnListItem();
        writer.OnInt64Scalar(std::numeric_limits<i64>::min());
        writer.OnListItem();
        writer.OnUint64Scalar(std::numeric_limits<ui64>::max());
        writer.OnListItem();
        writer.OnDoubleScalar(1.25);
        writer.OnListItem();
        writer.OnBooleanScalar(true);
        writer.OnListItem();
        writer.OnEntity();
    }
    writer.OnEndList();

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnEndMap();

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, OneItemMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnKeyedItem("hello");
    writer.OnStringScalar("world");
    writer.OnEndMap();

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, MapWithAttributes)
{
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("read"));
            EXPECT_CALL(Mock, OnBeginList());
                EXPECT_CALL(Mock, OnListItem());
                EXPECT_CALL(Mock, OnStringScalar("*"));
            EXPECT_CALL(Mock, OnEndList());

            EXPECT_CALL(Mock, OnKeyedItem("write"));
            EXPECT_CALL(Mock, OnBeginList());
                EXPECT_CALL(Mock, OnListItem());
                EXPECT_CALL(Mock, OnStringScalar("sandello"));
            EXPECT_CALL(Mock, OnEndList());
        EXPECT_CALL(Mock, OnEndMap());

        EXPECT_CALL(Mock, OnKeyedItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello"));

        EXPECT_CALL(Mock, OnKeyedItem("mode"));
        EXPECT_CALL(Mock, OnInt64Scalar(755));
    EXPECT_CALL(Mock, OnEndMap());

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("acl");
        writer.OnBeginMap();
            writer.OnKeyedItem("read");
            writer.OnBeginList();
                writer.OnListItem();
                writer.OnStringScalar("*");
            writer.OnEndList();

            writer.OnKeyedItem("write");
            writer.OnBeginList();
                writer.OnListItem();
                writer.OnStringScalar("sandello");
            writer.OnEndList();
        writer.OnEndMap();

        writer.OnKeyedItem("lock_scope");
        writer.OnStringScalar("mytables");
    writer.OnEndAttributes();

    writer.OnBeginMap();
        writer.OnKeyedItem("path");
        writer.OnStringScalar("/home/sandello");

        writer.OnKeyedItem("mode");
        writer.OnInt64Scalar(755);
    writer.OnEndMap();

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, AttributesWithUint64Scalar)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("key"));
    EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnUint64Scalar(2));

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Text);
    writer.OnBeginAttributes();
    writer.OnKeyedItem("key");
    writer.OnInt64Scalar(1);
    writer.OnEndAttributes();
    writer.OnUint64Scalar(2);

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, AttributesWithRawScalar)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("key"));
    EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnUint64Scalar(2));

    TBufferedYsonWriter writer(
        &Stream,
        EYsonFormat::Text,
        EYsonType::Node,
        /*enableRaw*/ true);
    writer.OnBeginAttributes();
    writer.OnKeyedItem("key");
    writer.OnInt64Scalar(1);
    writer.OnEndAttributes();
    writer.OnRaw("2u");

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, NestedAttributes)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnBeginAttributes());
            EXPECT_CALL(Mock, OnKeyedItem("b"));
            EXPECT_CALL(Mock, OnStringScalar("c"));
        EXPECT_CALL(Mock, OnEndAttributes());
        EXPECT_CALL(Mock, OnInt64Scalar(2));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnInt64Scalar(1));

    TBufferedYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("a");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("b");
            writer.OnStringScalar("c");
        writer.OnEndAttributes();
        writer.OnInt64Scalar(2);
    writer.OnEndAttributes();
    writer.OnInt64Scalar(1);

    Run(&writer);
}

TEST_F(TBufferedYsonWriterTest, UtfString)
{
    const std::string utfString("строка ютф и специальные символы - \n \t \b");
    TStringStream output;
    TBufferedYsonWriter writer(
        &output,
        EYsonFormat::Text,
        EYsonType::Node,
        /*enableRaw*/ true,
        TBufferedYsonWriter::DefaultIndent,
        /*passThroughUtf8Characters*/ true);
    writer.OnStringScalar(TStringBuf(utfString));
    writer.Flush();
    EXPECT_EQ("\"\xD1\x81\xD1\x82\xD1\x80\xD0\xBE\xD0\xBA\xD0\xB0 \xD1\x8E\xD1\x82\xD1\x84 \xD0\xB8 \xD1\x81\xD0\xBF\xD0\xB5\xD1\x86\xD0\xB8\xD0\xB0\xD0\xBB\xD1\x8C\xD0\xBD\xD1\x8B\xD0\xB5 \xD1\x81\xD0\xB8\xD0\xBC\xD0\xB2\xD0\xBE\xD0\xBB\xD1\x8B - \\n \\t \\x08\"", output.Str());
}

TEST_F(TBufferedYsonWriterTest, Escaping)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text);

    std::string input;
    for (int i = 0; i < 256; ++i) {
        input.push_back(static_cast<char>(i));
    }

    writer.OnStringScalar(input);
    writer.Flush();

    std::string output =
        "\"\\0\\1\\2\\3\\4\\5\\6\\7\\x08\\t\\n\\x0B\\x0C\\r\\x0E\\x0F"
        "\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B"
        "\\x1C\\x1D\\x1E\\x1F !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCD"
        "EFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        "\\x7F\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8A"
        "\\x8B\\x8C\\x8D\\x8E\\x8F\\x90\\x91\\x92\\x93\\x94\\x95\\x96"
        "\\x97\\x98\\x99\\x9A\\x9B\\x9C\\x9D\\x9E\\x9F\\xA0\\xA1\\xA2"
        "\\xA3\\xA4\\xA5\\xA6\\xA7\\xA8\\xA9\\xAA\\xAB\\xAC\\xAD\\xAE"
        "\\xAF\\xB0\\xB1\\xB2\\xB3\\xB4\\xB5\\xB6\\xB7\\xB8\\xB9\\xBA"
        "\\xBB\\xBC\\xBD\\xBE\\xBF\\xC0\\xC1\\xC2\\xC3\\xC4\\xC5\\xC6"
        "\\xC7\\xC8\\xC9\\xCA\\xCB\\xCC\\xCD\\xCE\\xCF\\xD0\\xD1\\xD2"
        "\\xD3\\xD4\\xD5\\xD6\\xD7\\xD8\\xD9\\xDA\\xDB\\xDC\\xDD\\xDE"
        "\\xDF\\xE0\\xE1\\xE2\\xE3\\xE4\\xE5\\xE6\\xE7\\xE8\\xE9\\xEA"
        "\\xEB\\xEC\\xED\\xEE\\xEF\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6"
        "\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"";

    EXPECT_EQ(output, outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, ShortStringEscaping)
{
    auto check = [] (const std::string& input) {
        TStringStream outputStream;
        TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text);

        writer.OnStringScalar(input);
        writer.Flush();

        TString expected = "\"";
        expected += ::EscapeC(TStringBuf(input));
        expected += '"';
        EXPECT_EQ(expected, outputStream.Str());
    };

    check({});
    for (int value = 0; value < 256; ++value) {
        check(std::string(1, static_cast<char>(value)));
    }
    check(std::string(15, 'a'));
    check(std::string(15, '\n'));
    check(std::string({static_cast<char>(1), '7'}));
    check(std::string({static_cast<char>(0x1f), 'A'}));
}

TEST_F(TBufferedYsonWriterTest, NoNewLinesInEmptyMap)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginMap();
    writer.OnEndMap();
    writer.Flush();

    EXPECT_EQ("{}", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, NoNewLinesInEmptyList)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginList();
    writer.OnEndList();
    writer.Flush();

    EXPECT_EQ("[]", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, PrettyFormat)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginAttributes();
        writer.OnKeyedItem("attr");
        writer.OnStringScalar("value");
    writer.OnEndAttributes();
    writer.OnBeginMap();
        writer.OnKeyedItem("key1");
        writer.OnStringScalar("value1");
        writer.OnKeyedItem("key2");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("other_attr");
            writer.OnStringScalar("other_value");
        writer.OnEndAttributes();
        writer.OnStringScalar("value2");
    writer.OnEndMap();
    writer.Flush();

    EXPECT_EQ(
        "<\n"
        "    \"attr\" = \"value\";\n"
        "> {\n"
        "    \"key1\" = \"value1\";\n"
        "    \"key2\" = <\n"
        "        \"other_attr\" = \"other_value\";\n"
        "    > \"value2\";\n"
        "}",
        outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, PrettyFormatWithNegativeIndent)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(
        &outputStream,
        EYsonFormat::Pretty,
        EYsonType::Node,
        /*enableRaw*/ false,
        /*indent*/ -1);
    writer.OnBeginList();
    writer.OnListItem();
    writer.OnStringScalar("value");
    writer.OnEndList();
    writer.Flush();

    EXPECT_EQ(
        "[\n"
        "\"value\";\n"
        "]",
        outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, PrettyFormatWithLongIndent)
{
    constexpr int Indent = 100;

    TStringStream outputStream;
    TBufferedYsonWriter writer(
        &outputStream,
        EYsonFormat::Pretty,
        EYsonType::Node,
        /*enableRaw*/ false,
        Indent);
    writer.OnBeginList();
    writer.OnListItem();
    writer.OnStringScalar("value");
    writer.OnEndList();
    writer.Flush();

    EXPECT_EQ("[\n" + std::string(Indent, ' ') + "\"value\";\n]", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, PrettyFormatDoesNotIndentAtNegativeDepth)
{
    TSizeLimitedOutputStream outputStream(64);
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Pretty);

    EXPECT_NO_THROW({
        writer.OnEndMap();
        writer.OnEndList();
        writer.Flush();
    });
    EXPECT_EQ(writer.GetDepth(), -2);
    EXPECT_LT(outputStream.GetWrittenSize(), 64u);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBufferedYsonWriterTest, CompletedTopLevelNodeStaysBuffered)
{
    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text);
    writer.OnBeginMap();
    writer.OnKeyedItem("key");
    writer.OnStringScalar("value");
    writer.OnEndMap();

    EXPECT_EQ("", outputStream.Str());

    writer.Flush();
    EXPECT_EQ("{\"key\"=\"value\";}", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, FlushWhileWritingList)
{
    const std::string value(512, 'a');

    TStringStream outputStream;
    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text);
    writer.OnBeginList();
    writer.OnListItem();
    writer.OnStringScalar(value);

    writer.Flush();
    EXPECT_EQ("[\"" + value + "\";", outputStream.Str());

    writer.OnListItem();
    writer.OnStringScalar("tail");
    writer.OnEndList();
    writer.Flush();
    EXPECT_EQ("[\"" + value + "\";\"tail\";]", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, DestructorFlushesBufferedOutput)
{
    TStringStream outputStream;
    {
        TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text);
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnStringScalar("value");
    }

    EXPECT_EQ("[\"value\";", outputStream.Str());
}

TEST_F(TBufferedYsonWriterTest, FlushRestoresBufferInvariantAfterWriteFailure)
{
    TThrowingOutputStream outputStream;
    TBufferedYsonWriter writer(
        &outputStream,
        EYsonFormat::Text,
        EYsonType::ListFragment,
        /*enableRaw*/ true);

    writer.OnRaw(std::string(63, 'a'), EYsonType::ListFragment);
    EXPECT_THROW(writer.OnRaw("b", EYsonType::ListFragment), std::runtime_error);
    EXPECT_EQ(std::vector<size_t>{64}, outputStream.GetWriteLengths());

    EXPECT_NO_THROW(writer.OnRaw("c", EYsonType::ListFragment));
    EXPECT_EQ(std::vector<size_t>{64}, outputStream.GetWriteLengths());
}

TEST_F(TBufferedYsonWriterTest, RawListFragmentChunksCrossFlushBoundaries)
{
    constexpr int MaxLength = 256;

    std::array<std::string, MaxLength + 1> firstChunks;
    std::array<std::string, MaxLength + 1> secondChunks;
    for (int length = 0; length <= MaxLength; ++length) {
        firstChunks[length] = std::string(length, ' ');
        secondChunks[length] = std::string(length, '\n');
    }

    for (int firstLength = 0; firstLength <= MaxLength; ++firstLength) {
        for (int secondLength = 0; secondLength <= MaxLength; ++secondLength) {
            TStringStream outputStream;
            TBufferedYsonWriter writer(
                &outputStream,
                EYsonFormat::Text,
                EYsonType::ListFragment,
                /*enableRaw*/ true);
            writer.OnRaw(firstChunks[firstLength], EYsonType::ListFragment);
            writer.OnRaw(secondChunks[secondLength], EYsonType::ListFragment);
            writer.Flush();

            EXPECT_EQ(firstChunks[firstLength] + secondChunks[secondLength], outputStream.Str())
                << "firstLength: " << firstLength << ", secondLength: " << secondLength;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBufferedYsonFragmentWriterTest, NewLinesInList)
{
    TStringStream outputStream;

    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::ListFragment);
    writer.OnListItem();
        writer.OnInt64Scalar(200);
    writer.OnListItem();
        writer.OnBeginMap();
            writer.OnKeyedItem("key");
            writer.OnInt64Scalar(42);
            writer.OnKeyedItem("yek");
            writer.OnInt64Scalar(24);
            writer.OnKeyedItem("list");
            writer.OnBeginList();
            writer.OnEndList();
        writer.OnEndMap();
    writer.OnListItem();
        writer.OnStringScalar("aaa");
    writer.OnListItem();
        writer.OnStringScalar("bbb");
    writer.Flush();

    std::string output =
        "200;\n"
        "{\"key\"=42;\"yek\"=24;\"list\"=[];};\n"
        "\"aaa\";\n"
        "\"bbb\";\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TBufferedYsonFragmentWriterTest, BinaryList)
{
    TStringStream outputStream;

    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Binary, EYsonType::ListFragment);
    writer.OnListItem();
        writer.OnInt64Scalar(200);
    writer.OnListItem();
        writer.OnStringScalar("aaa");
    writer.Flush();

    std::string output =
        "\x2\x90\x3;"
        "\x1\x6""aaa;";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TBufferedYsonFragmentWriterTest, NewLinesInMap)
{
    TStringStream outputStream;

    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::MapFragment);
    writer.OnKeyedItem("a");
        writer.OnInt64Scalar(100);
    writer.OnKeyedItem("b");
        writer.OnBeginList();
            writer.OnListItem();
            writer.OnBeginMap();
                writer.OnKeyedItem("key");
                writer.OnInt64Scalar(42);
                writer.OnKeyedItem("yek");
                writer.OnInt64Scalar(24);
            writer.OnEndMap();
            writer.OnListItem();
            writer.OnInt64Scalar(-1);
        writer.OnEndList();
    writer.OnKeyedItem("c");
        writer.OnStringScalar("word");
    writer.Flush();

    std::string output =
        "\"a\"=100;\n"
        "\"b\"=[{\"key\"=42;\"yek\"=24;};-1;];\n"
        "\"c\"=\"word\";\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TBufferedYsonFragmentWriterTest, NoFirstIndent)
{
    TStringStream outputStream;

    TBufferedYsonWriter writer(&outputStream, EYsonFormat::Pretty, EYsonType::MapFragment);
    writer.OnKeyedItem("a1");
        writer.OnBeginMap();
            writer.OnKeyedItem("key");
            writer.OnInt64Scalar(42);
        writer.OnEndMap();
    writer.OnKeyedItem("a2");
        writer.OnInt64Scalar(0);
    writer.Flush();

    std::string output =
        "\"a1\" = {\n"
        "    \"key\" = 42;\n"
        "};\n"
        "\"a2\" = 0;\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
