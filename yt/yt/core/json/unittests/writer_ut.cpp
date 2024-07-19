#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/config.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/strip.h>

namespace NYT::NJson {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

inline TString SurroundWithQuotes(const TString& s)
{
    TString quote = "\"";
    return quote + s + quote;
}

TEST(TJsonWriterTest, Basic)
{
    auto getWrittenJson = [] (bool pretty) {
        TStringStream outputStream;
        auto writer = CreateJsonWriter(&outputStream, pretty);

        BuildYsonFluently(writer.get())
            .BeginList()
                .Item().Value(123)
                .Item().Value(-56)
                .Item()
                .BeginMap()
                    .Item("key").Value(true)
                    .Item("entity").Entity()
                    .Item("value").Value(4.25)
                .EndMap()
                .Item().Value(std::numeric_limits<double>::infinity())
            .EndList();
        writer->Flush();
        return outputStream.Str();
    };

    EXPECT_EQ(getWrittenJson(false), R"([123,-56,{"key":true,"entity":null,"value":4.25},inf])");
    EXPECT_EQ(Strip(getWrittenJson(true)),
        "[\n"
        "    123,\n"
        "    -56,\n"
        "    {\n"
        "        \"key\": true,\n"
        "        \"entity\": null,\n"
        "        \"value\": 4.25\n"
        "    },\n"
        "    inf\n"
        "]");
}

TEST(TJsonWriterTest, StartNextValue)
{
    TStringStream outputStream;
    {
        auto writer = CreateJsonWriter(&outputStream);

        BuildYsonFluently(writer.get())
            .BeginList()
                .Item().Value(123)
                .Item().Value("hello")
            .EndList();

        writer->StartNextValue();

        BuildYsonFluently(writer.get())
            .BeginMap()
                .Item("abc").Value(true)
                .Item("def").Value(-1.5)
            .EndMap();

        writer->Flush();
    }

    EXPECT_EQ(outputStream.Str(), "[123,\"hello\"]\n{\"abc\":true,\"def\":-1.5}");
}

TEST(TJsonWriterTest, Errors)
{
    TStringStream outputStream;

    {
        auto writer = CreateJsonWriter(&outputStream);
        // Non-UTF-8.
        EXPECT_THROW(writer->OnStringScalar("\xFF\xFE\xFC"), TErrorException);
    }
    {
        auto writer = CreateJsonWriter(&outputStream);
        EXPECT_THROW(writer->OnBeginAttributes(), TErrorException);
    }
    {
        auto writer = CreateJsonWriter(&outputStream);
        EXPECT_THROW(writer->OnRaw("{x=3}", EYsonType::Node), TErrorException);
    }
}

////////////////////////////////////////////////////////////////////////////////

// Basic types.
TEST(TJsonConsumerTest, List)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginList();
        consumer->OnListItem();
        consumer->OnInt64Scalar(1);
        consumer->OnListItem();
        consumer->OnStringScalar("aaa");
        consumer->OnListItem();
        consumer->OnDoubleScalar(3.5);
    consumer->OnEndList();
    consumer->Flush();

    TString output = "[1,\"aaa\",3.5]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, Map)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":\"world\",\"foo\":\"bar\"}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, DoubleMap)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream, NYson::EYsonType::ListFragment);

    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
    consumer->OnEndMap();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, ListFragmentWithEntity)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream, NYson::EYsonType::ListFragment);

    consumer->OnListItem();
    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("x");
        consumer->OnStringScalar("y");
    consumer->OnEndAttributes();
    consumer->OnEntity();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
    consumer->OnEndMap();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"$attributes\":{\"x\":\"y\"},\"$value\":null}\n{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, Entity)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnEntity();
    consumer->Flush();

    TString output = "null";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, SupportInfinity)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->SupportInfinity = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
    consumer->OnKeyedItem("a");
    consumer->OnDoubleScalar(-std::numeric_limits<double>::infinity());
    consumer->OnKeyedItem("b");
    consumer->OnDoubleScalar(std::numeric_limits<double>::infinity());
    consumer->OnEndMap();

    consumer->Flush();

    TString output = "{\"a\":-inf,\"b\":inf}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, StringifyNanAndInfinity)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringifyNanAndInfinity = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
    consumer->OnKeyedItem("a");
    consumer->OnDoubleScalar(-std::numeric_limits<double>::infinity());
    consumer->OnKeyedItem("b");
    consumer->OnDoubleScalar(std::numeric_limits<double>::infinity());
    consumer->OnKeyedItem("c");
    consumer->OnDoubleScalar(std::numeric_limits<double>::quiet_NaN());
    consumer->OnEndMap();

    consumer->Flush();

    TString output = "{\"a\":\"-inf\",\"b\":\"inf\",\"c\":\"nan\"}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, EmptyString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnStringScalar("");
    consumer->Flush();

    TString output = SurroundWithQuotes("");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, AsciiString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    TString s = TString("\x7F\x32", 2);
    consumer->OnStringScalar(s);
    consumer->Flush();

    TString output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}


TEST(TJsonConsumerTest, NonAsciiString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    TString s = TString("\xFF\x00\x80", 3);
    consumer->OnStringScalar(s);
    consumer->Flush();

    TString output = SurroundWithQuotes("\xC3\xBF\\u0000\xC2\x80");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, NonAsciiStringWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    TString s = TString("\xC3\xBF", 2);
    consumer->OnStringScalar(s);
    consumer->Flush();

    TString output = SurroundWithQuotes(TString("\xC3\xBF", 2));
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, IncorrectUtfWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    TString s = TString("\xFF", 1);
    EXPECT_ANY_THROW(
        consumer->OnStringScalar(s););
}

TEST(TJsonConsumerTest, StringStartingWithSpecialSymbol)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    TString s = "&some_string";
    consumer->OnStringScalar(s);
    consumer->Flush();

    TString output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes.
TEST(TJsonConsumerTest, ListWithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginList();
        consumer->OnListItem();
        consumer->OnInt64Scalar(1);
    consumer->OnEndList();
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":[1]"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, MapWithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("spam");
        consumer->OnStringScalar("bad");
    consumer->OnEndMap();
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":{\"spam\":\"bad\"}"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, Int64WithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnInt64Scalar(42);
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":42"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, Uint64)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnUint64Scalar(42);
    consumer->Flush();

    TString output = "42";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, EntityWithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnEntity();
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":null"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, StringWithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnStringScalar("some_string");
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":\"some_string\""
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, DoubleAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("another_foo");
            consumer->OnStringScalar("another_bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnStringScalar("some_string");
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":"
                "{"
                    "\"$attributes\":{\"another_foo\":\"another_bar\"}"
                    ","
                    "\"$value\":\"bar\"}"
                "}"
            ","
            "\"$value\":\"some_string\""
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJsonConsumerTest, NeverAttributes)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AttributesMode = EJsonAttributesMode::Never;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("answer");
        consumer->OnInt64Scalar(42);

        consumer->OnKeyedItem("question");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("foo");
            consumer->OnStringScalar("bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("strange question");
    consumer->OnEndMap();
    consumer->Flush();

    TString output =
        "{"
            "\"answer\":42,"
            "\"question\":\"strange question\""
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, AlwaysAttributes)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AttributesMode = EJsonAttributesMode::Always;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("answer");
        consumer->OnInt64Scalar(42);

        consumer->OnKeyedItem("question");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("foo");
            consumer->OnStringScalar("bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("strange question");
    consumer->OnEndMap();
    consumer->Flush();

    TString output =
        "{"
            "\"$attributes\":{\"foo\":{\"$attributes\":{},\"$value\":\"bar\"}},"
            "\"$value\":"
            "{"
                "\"answer\":{\"$attributes\":{},\"$value\":42},"
                "\"question\":"
                "{"
                    "\"$attributes\":{\"foo\":{\"$attributes\":{},\"$value\":\"bar\"}},"
                    "\"$value\":\"strange question\""
                "}"
            "}"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, SpecialKeys)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("$value");
        consumer->OnStringScalar("foo");
        consumer->OnKeyedItem("$$attributes");
        consumer->OnStringScalar("bar");
        consumer->OnKeyedItem("$other");
        consumer->OnInt64Scalar(42);
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"$$value\":\"foo\",\"$$$attributes\":\"bar\",\"$$other\":42}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, TestStringLengthLimit)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringLengthLimit = 2;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar(TString(10000, 'A'));
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$incomplete\":true,\"$value\":\"AA\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, TestAnnotateWithTypes)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$type\":\"string\",\"$value\":\"world\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, TestAnnotateWithTypesStringify)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    config->Stringify = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnUint64Scalar(-1);
        consumer->OnKeyedItem("world");
        consumer->OnDoubleScalar(1.7976931348623157e+308);
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$type\":\"uint64\",\"$value\":\"18446744073709551615\"},"
        "\"world\":{\"$type\":\"double\",\"$value\":\"1.7976931348623157e+308\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, SeveralOptions)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringLengthLimit = 2;
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar(TString(10000, 'A'));
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$incomplete\":true,\"$type\":\"string\",\"$value\":\"AA\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, SeveralOptions2)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringLengthLimit = 4;
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("mood");
            consumer->OnInt64Scalar(42);
        consumer->OnEndAttributes();
        consumer->OnStringScalar(TString(10000, 'A'));
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$attributes\":{\"mood\":{\"$type\":\"int64\",\"$value\":42}},"
        "\"$incomplete\":true,\"$type\":\"string\",\"$value\":\"AAAA\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, SeveralOptionsFlushBuffer)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringLengthLimit = 2;
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::ListFragment, config);

    consumer->OnListItem();
    consumer->OnStringScalar(TString(10000, 'A'));
    consumer->Flush();

    TString output = "{\"$incomplete\":true,\"$type\":\"string\",\"$value\":\"AA\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonConsumerTest, TestPrettyFormat)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->Format = EJsonFormat::Pretty;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnInt64Scalar(1);
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\n"
                    "    \"hello\": 1\n"
                    "}";
    EXPECT_EQ(output, StripInPlace(outputStream.Str()));
}

TEST(TJsonConsumerTest, TestNodeWeightLimitAccepted)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, std::move(config));

    TString yson = "<\"attr\"=123>\"456\"";
    consumer->OnNodeWeightLimited(yson, yson.Size() - 1);
    consumer->Flush();

    TString expectedOutput =
        "{"
            "\"$incomplete\":true,"
            "\"$type\":\"any\","
            "\"$value\":\"\""
        "}";
    EXPECT_EQ(expectedOutput, outputStream.Str());
}

TEST(TJsonConsumerTest, TestNodeWeightLimitRejected)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    TString yson = "<\"attr\"=123>\"456\"";
    consumer->OnNodeWeightLimited(yson, yson.Size());
    consumer->Flush();

    TString expectedOutput =
        "{"
            "\"$attributes\":{"
                "\"attr\":123"
            "},"
            "\"$value\":\"456\""
        "}";
    EXPECT_EQ(expectedOutput, outputStream.Str());
}

TEST(TJsonConsumerTest, TestStringScalarWeightLimitAccepted)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, std::move(config));

    consumer->OnStringScalarWeightLimited("1234567", 5);
    consumer->Flush();

    TString expectedOutput =
        "{"
            "\"$incomplete\":true,"
            "\"$type\":\"string\","
            "\"$value\":\"12345\""
        "}";
    EXPECT_EQ(expectedOutput, outputStream.Str());
}

TEST(TJsonConsumerTest, TestStringScalarWeightLimitRejected)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    TString value = "1234567";
    consumer->OnStringScalarWeightLimited(value, value.size());
    consumer->Flush();

    EXPECT_EQ(SurroundWithQuotes(value), outputStream.Str());
}

TEST(TJsonConsumerTest, TestSetAnnotateWithTypesParameter)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, std::move(config));

    consumer->OnBeginList();

    consumer->OnListItem();
    consumer->OnStringScalar("1234567");

    consumer->SetAnnotateWithTypesParameter(false);
    consumer->OnListItem();
    consumer->OnStringScalar("1234567");

    consumer->OnEndList();

    consumer->Flush();

    TString expectedOutput =
        "["
            "{"
                "\"$type\":\"string\","
                "\"$value\":\"1234567\""
            "},"
            "\"1234567\""
        "]";

    EXPECT_EQ(expectedOutput, outputStream.Str());
}

TEST(TJsonConsumerTest, ThroughJsonWriter)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AnnotateWithTypes = true;
    config->Stringify = true;

    auto writer = CreateJsonWriter(&outputStream);
    auto consumer = CreateJsonConsumer(writer.get(), EYsonType::Node, config);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnUint64Scalar(-1);
        consumer->OnKeyedItem("world");
        consumer->OnDoubleScalar(1.7976931348623157e+308);
    consumer->OnEndMap();
    consumer->Flush();

    TString output = "{\"hello\":{\"$type\":\"uint64\",\"$value\":\"18446744073709551615\"},"
        "\"world\":{\"$type\":\"double\",\"$value\":\"1.7976931348623157e+308\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJson
