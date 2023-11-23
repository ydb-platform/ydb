#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/stream.h>

#include <util/string/escape.h>

namespace NYT::NYson {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TYsonWriterTest
    : public ::testing::Test
{
protected:
    TStringStream Stream;
    StrictMock<TMockYsonConsumer> Mock;

    void Run()
    {
        Stream.Flush();
        ParseYson(TYsonInput(&Stream), &Mock);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define TEST_SCALAR(value, type) \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TYsonWriter writer(&Stream, EYsonFormat::Binary); \
        writer.On##type##Scalar(value); \
        Run(); \
    } \
        \
    { \
        InSequence dummy; \
        EXPECT_CALL(Mock, On##type##Scalar(value)); \
        TYsonWriter writer(&Stream, EYsonFormat::Text); \
        writer.On##type##Scalar(value); \
        Run(); \
    } \


TEST_F(TYsonWriterTest, String)
{
    TString value = "YSON";
    TEST_SCALAR(value, String)
}

TEST_F(TYsonWriterTest, Int64)
{
    i64 value = 100500424242ll;
    TEST_SCALAR(value, Int64)
}

TEST_F(TYsonWriterTest, Uint64)
{
    ui64 value = 100500424242llu;
    TEST_SCALAR(value, Uint64)
}

TEST_F(TYsonWriterTest, Boolean)
{
    bool value = true;
    TEST_SCALAR(value, Boolean)
}

TEST_F(TYsonWriterTest, Double)
{
    double value = 1.7976931348623157e+308;
    TEST_SCALAR(value, Double)
}

TEST_F(TYsonWriterTest, MinusInf)
{
    double value = std::numeric_limits<double>::infinity();
    TEST_SCALAR(value, Double)
}

TEST_F(TYsonWriterTest, NaN)
{
    InSequence dummy;
    TYsonWriter writer(&Stream, EYsonFormat::Text);
    writer.OnDoubleScalar(std::numeric_limits<double>::quiet_NaN());
    Stream.Flush();
    EXPECT_EQ(Stream.Str(), "%nan");
}

TEST_F(TYsonWriterTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, OneItemMap)
{

    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("hello"));
    EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginMap();
    writer.OnKeyedItem("hello");
    writer.OnStringScalar("world");
    writer.OnEndMap();

    Run();
}

TEST_F(TYsonWriterTest, MapWithAttributes)
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

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

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

    Run();
}

TEST_F(TYsonWriterTest, UtfString)
{
    const TString utfString("строка ютф и специальные символы - \n \t \b");
    TStringStream output;
    TYsonWriter writer(
        &output,
        EYsonFormat::Text,
        EYsonType::Node,
        true,
        TYsonWriter::DefaultIndent,
        /*passThroughUtf8Characters*/ true);
    writer.OnStringScalar(TStringBuf(utfString));
    EXPECT_EQ("\"\xD1\x81\xD1\x82\xD1\x80\xD0\xBE\xD0\xBA\xD0\xB0 \xD1\x8E\xD1\x82\xD1\x84 \xD0\xB8 \xD1\x81\xD0\xBF\xD0\xB5\xD1\x86\xD0\xB8\xD0\xB0\xD0\xBB\xD1\x8C\xD0\xBD\xD1\x8B\xD0\xB5 \xD1\x81\xD0\xB8\xD0\xBC\xD0\xB2\xD0\xBE\xD0\xBB\xD1\x8B - \\n \\t \\x08\"", output.Str());
}

TEST_F(TYsonWriterTest, Escaping)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Text);

    TString input;
    for (int i = 0; i < 256; ++i) {
        input.push_back(char(i));
    }

    writer.OnStringScalar(input);

    TString output =
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

TEST_F(TYsonWriterTest, ConvertToYson)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Text);

    TString input;
    for (int i = 0; i < 256; ++i) {
        input.push_back(char(i));
    }

    writer.OnStringScalar(input);

    TString output =
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

TEST_F(TYsonWriterTest, NoNewLinesInEmptyMap)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginMap();
    writer.OnEndMap();

    EXPECT_EQ("{}", outputStream.Str());
}

TEST_F(TYsonWriterTest, NoNewLinesInEmptyList)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    writer.OnBeginList();
    writer.OnEndList();

    EXPECT_EQ("[]", outputStream.Str());
}

TEST_F(TYsonWriterTest, NestedAttributes)
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

    TYsonWriter writer(&Stream, EYsonFormat::Binary);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("a");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("b");
            writer.OnStringScalar("c");
        writer.OnEndAttributes();
        writer.OnInt64Scalar(2);
    writer.OnEndAttributes();
    writer.OnInt64Scalar(1);

    Run();
}

TEST_F(TYsonWriterTest, PrettyFormat)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
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

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonFragmentWriterTest, NewLinesInList)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::ListFragment);
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

    TString output =
        "200;\n"
        "{\"key\"=42;\"yek\"=24;\"list\"=[];};\n"
        "\"aaa\";\n"
        "\"bbb\";\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYsonFragmentWriterTest, BinaryList)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Binary, EYsonType::ListFragment);
    writer.OnListItem();
        writer.OnInt64Scalar(200);
    writer.OnListItem();
        writer.OnStringScalar("aaa");

    TString output =
        "\x2\x90\x3;"
        "\x1\x6""aaa;";

    EXPECT_EQ(output, outputStream.Str());
}


TEST(TYsonFragmentWriterTest, NewLinesInMap)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Text, EYsonType::MapFragment);
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

    TString output =
        "\"a\"=100;\n"
        "\"b\"=[{\"key\"=42;\"yek\"=24;};-1;];\n"
        "\"c\"=\"word\";\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYsonFragmentWriterTest, NoFirstIndent)
{
    TStringStream outputStream;

    TYsonWriter writer(&outputStream, EYsonFormat::Pretty, EYsonType::MapFragment);
    writer.OnKeyedItem("a1");
        writer.OnBeginMap();
            writer.OnKeyedItem("key");
            writer.OnInt64Scalar(42);
        writer.OnEndMap();
    writer.OnKeyedItem("a2");
        writer.OnInt64Scalar(0);

    TString output =
        "\"a1\" = {\n"
        "    \"key\" = 42;\n"
        "};\n"
        "\"a2\" = 0;\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
